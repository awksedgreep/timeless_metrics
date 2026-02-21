defmodule TimelessMetrics.DictTrainer do
  @moduledoc """
  Manages zstd dictionaries for improved compression of Gorilla-compressed segments.

  On startup, loads any existing dictionary from `data_dir/dict_v*.zstd`.
  Stores compiled cdict/ddict in persistent_term for zero-cost access
  during compression and decompression.

  Dictionaries are trained via `mix tm.train_dict` or `train/2`.
  """

  use GenServer

  require Logger

  @dict_max_size 32_768
  @default_sample_count 500

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Get the compiled compression dictionary for a store, or nil if none."
  def get_cdict(store) do
    :persistent_term.get({TimelessMetrics, store, :cdict}, nil)
  end

  @doc "Get the compiled decompression dictionary for a store, or nil if none."
  def get_ddict(store) do
    :persistent_term.get({TimelessMetrics, store, :ddict}, nil)
  end

  @doc "Get the raw dictionary binary for a store, or nil if none."
  def get_dict(store) do
    :persistent_term.get({TimelessMetrics, store, :dict_binary}, nil)
  end

  @doc "Get the dictionary version for a store, or 0 if none."
  def get_dict_version(store) do
    :persistent_term.get({TimelessMetrics, store, :dict_version}, 0)
  end

  @doc """
  Train a dictionary from existing segment data.

  Reads Gorilla-compressed blobs (pre-zstd) from the store's segments,
  uses them as training samples for zstd dictionary building.

  Options:
    * `:sample_count` - Number of segments to sample (default: 500)
    * `:compression_level` - zstd compression level for the cdict (default: 9)
    * `:version` - Dictionary version number (default: auto-increment)
  """
  def train(store, opts \\ []) do
    trainer_name = :"#{store}_dict_trainer"

    if Process.whereis(trainer_name) do
      GenServer.call(trainer_name, {:train, opts}, :timer.minutes(5))
    else
      do_train(store, opts)
    end
  end

  # --- Server ---

  @impl true
  def init(opts) do
    store = Keyword.fetch!(opts, :store)
    data_dir = Keyword.fetch!(opts, :data_dir)

    state = %{
      store: store,
      data_dir: data_dir
    }

    # Try to load existing dictionary
    case find_latest_dict(data_dir) do
      {:ok, version, dict_binary} ->
        load_dict(store, dict_binary, version, 9)

        Logger.info(
          "Loaded zstd dictionary v#{version} for #{store} (#{byte_size(dict_binary)} bytes)"
        )

      :none ->
        :ok
    end

    {:ok, state}
  end

  @impl true
  def handle_call({:train, opts}, _from, state) do
    result = do_train(state.store, Keyword.put(opts, :data_dir, state.data_dir))
    {:reply, result, state}
  end

  # --- Training ---

  defp do_train(store, opts) do
    sample_count = Keyword.get(opts, :sample_count, @default_sample_count)
    compression_level = Keyword.get(opts, :compression_level, 9)
    data_dir = Keyword.get(opts, :data_dir, get_data_dir(store))

    if data_dir == nil do
      {:error, "No data_dir available. Pass :data_dir option or start DictTrainer."}
    else
      samples = collect_samples(data_dir, sample_count)

      if length(samples) < 10 do
        {:error, "Not enough samples (#{length(samples)}). Need at least 10 segments."}
      else
        case train_dict_from_samples(samples) do
          {:ok, dict_binary} ->
            version = Keyword.get(opts, :version, get_dict_version(store) + 1)
            save_dict(data_dir, version, dict_binary)
            load_dict(store, dict_binary, version, compression_level)

            Logger.info(
              "Trained zstd dictionary v#{version} for #{store}: " <>
                "#{byte_size(dict_binary)} bytes from #{length(samples)} samples"
            )

            {:ok,
             %{version: version, dict_size: byte_size(dict_binary), sample_count: length(samples)}}

          {:error, reason} ->
            {:error, reason}
        end
      end
    end
  end

  defp get_data_dir(store) do
    try do
      :persistent_term.get({TimelessMetrics, store, :data_dir}, nil)
    rescue
      _ -> nil
    end
  end

  @doc false
  def collect_samples(data_dir, max_samples) do
    # Read Gorilla+zstd compressed blobs from .seg files and WAL
    shard_dirs =
      case File.ls(data_dir) do
        {:ok, files} ->
          files
          |> Enum.filter(&String.starts_with?(&1, "shard_"))
          |> Enum.map(&Path.join(data_dir, &1))

        {:error, _} ->
          []
      end

    samples =
      shard_dirs
      |> Enum.flat_map(fn shard_dir ->
        raw_dir = Path.join(shard_dir, "raw")

        if File.dir?(raw_dir) do
          seg_blobs =
            case File.ls(raw_dir) do
              {:ok, files} ->
                files
                |> Enum.filter(&String.ends_with?(&1, ".seg"))
                |> Enum.flat_map(fn seg_file ->
                  read_seg_blobs(Path.join(raw_dir, seg_file))
                end)

              {:error, _} ->
                []
            end

          # Also read from WAL (in-progress segments not yet sealed)
          wal_blobs = read_wal_blobs(Path.join(raw_dir, "current.wal"))

          seg_blobs ++ wal_blobs
        else
          []
        end
      end)
      |> Enum.take(max_samples)

    samples
  end

  defp read_wal_blobs(wal_path) do
    case File.read(wal_path) do
      {:ok, data} -> decode_wal_blobs(data)
      {:error, _} -> []
    end
  end

  defp decode_wal_blobs(<<>>), do: []

  defp decode_wal_blobs(<<
         _sid::signed-64,
         _start::signed-64,
         _end_t::signed-64,
         _count::unsigned-32,
         data_len::unsigned-32,
         data::binary-size(data_len),
         rest::binary
       >>) do
    [data | decode_wal_blobs(rest)]
  end

  defp decode_wal_blobs(_), do: []

  defp read_seg_blobs(seg_path) do
    case File.read(seg_path) do
      {:ok, data} when byte_size(data) > 15 ->
        parse_seg_blobs(data)

      _ ->
        []
    end
  end

  defp parse_seg_blobs(data) do
    # .seg format: "TS" (2) | version (1) | count (4) | index_offset (8) | data... | index...
    case data do
      <<"TS", _version, count::unsigned-big-32, index_offset::unsigned-big-64, _rest::binary>> ->
        if index_offset + count * 40 <= byte_size(data) do
          # Read index entries to find blob offsets and lengths
          index_start = index_offset
          header_size = 15

          for i <- 0..(count - 1), reduce: [] do
            acc ->
              entry_offset = index_start + i * 40

              <<_pre::binary-size(entry_offset), _sid::64, _start::64, _end::64, _count::32,
                offset::unsigned-big-64, len::unsigned-big-32, _rest::binary>> = data

              blob_start = header_size + offset

              if blob_start + len <= byte_size(data) and len > 0 do
                blob = :binary.part(data, blob_start, len)
                [blob | acc]
              else
                acc
              end
          end
        else
          []
        end

      _ ->
        []
    end
  end

  defp train_dict_from_samples(samples) do
    # Use zstd CLI for training since ezstd doesn't expose ZDICT_trainFromBuffer
    tmp_dir = Path.join(System.tmp_dir!(), "tm_dict_train_#{System.os_time(:millisecond)}")
    File.mkdir_p!(tmp_dir)

    try do
      # Write samples as individual files
      Enum.with_index(samples, fn sample, i ->
        File.write!(Path.join(tmp_dir, "sample_#{i}.bin"), sample)
      end)

      dict_path = Path.join(tmp_dir, "trained.dict")

      case System.cmd(
             "zstd",
             [
               "--train",
               "--maxdict=#{@dict_max_size}",
               "-o",
               dict_path
               | Enum.map(0..(length(samples) - 1), &Path.join(tmp_dir, "sample_#{&1}.bin"))
             ],
             stderr_to_stdout: true
           ) do
        {_output, 0} ->
          case File.read(dict_path) do
            {:ok, dict_binary} -> {:ok, dict_binary}
            {:error, reason} -> {:error, "Failed to read trained dictionary: #{inspect(reason)}"}
          end

        {output, code} ->
          {:error, "zstd --train failed (exit #{code}): #{String.slice(output, 0, 500)}"}
      end
    after
      File.rm_rf!(tmp_dir)
    end
  end

  defp load_dict(store, dict_binary, version, compression_level) do
    cdict = :ezstd.create_cdict(dict_binary, compression_level)
    ddict = :ezstd.create_ddict(dict_binary)

    :persistent_term.put({TimelessMetrics, store, :cdict}, cdict)
    :persistent_term.put({TimelessMetrics, store, :ddict}, ddict)
    :persistent_term.put({TimelessMetrics, store, :dict_binary}, dict_binary)
    :persistent_term.put({TimelessMetrics, store, :dict_version}, version)
  end

  defp save_dict(data_dir, version, dict_binary) do
    dict_path = Path.join(data_dir, "dict_v#{version}.zstd")
    File.write!(dict_path, dict_binary)
  end

  defp find_latest_dict(data_dir) do
    case File.ls(data_dir) do
      {:ok, files} ->
        dict_files =
          files
          |> Enum.filter(&String.match?(&1, ~r/^dict_v\d+\.zstd$/))
          |> Enum.sort()

        case List.last(dict_files) do
          nil ->
            :none

          filename ->
            [version_str] = Regex.run(~r/\d+/, filename)
            version = String.to_integer(version_str)
            dict_binary = File.read!(Path.join(data_dir, filename))
            {:ok, version, dict_binary}
        end

      {:error, _} ->
        :none
    end
  end
end
