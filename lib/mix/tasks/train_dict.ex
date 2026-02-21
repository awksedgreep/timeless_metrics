defmodule Mix.Tasks.Tm.TrainDict do
  @moduledoc """
  Train a zstd dictionary from existing TimelessMetrics segment data.

  ## Usage

      mix tm.train_dict --data-dir /var/lib/timeless_metrics --store my_store

  ## Options

    * `--data-dir` - Path to the TimelessMetrics data directory (required)
    * `--store` - Store name atom (default: metrics)
    * `--samples` - Number of segments to sample (default: 500)
    * `--level` - zstd compression level for the cdict (default: 9)
  """

  use Mix.Task

  @shortdoc "Train a zstd dictionary from existing segment data"

  @impl true
  def run(args) do
    {opts, _, _} =
      OptionParser.parse(args,
        strict: [
          data_dir: :string,
          store: :string,
          samples: :integer,
          level: :integer
        ]
      )

    data_dir = opts[:data_dir] || raise "Missing --data-dir"
    store = String.to_atom(opts[:store] || "metrics")
    sample_count = opts[:samples] || 500
    level = opts[:level] || 9

    Mix.shell().info("Collecting samples from #{data_dir}...")

    samples = TimelessMetrics.DictTrainer.collect_samples(data_dir, sample_count)
    Mix.shell().info("Found #{length(samples)} segment blobs")

    if length(samples) < 10 do
      Mix.shell().error("Not enough samples (need at least 10). Write more data first.")
    else
      case TimelessMetrics.DictTrainer.train(store,
             data_dir: data_dir,
             sample_count: sample_count,
             compression_level: level
           ) do
        {:ok, info} ->
          Mix.shell().info(
            "Dictionary trained successfully!\n" <>
              "  Version: #{info.version}\n" <>
              "  Size: #{info.dict_size} bytes\n" <>
              "  Samples: #{info.sample_count}\n" <>
              "  Saved to: #{Path.join(data_dir, "dict_v#{info.version}.zstd")}"
          )

        {:error, reason} ->
          Mix.shell().error("Training failed: #{reason}")
      end
    end
  end
end
