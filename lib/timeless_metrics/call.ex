defmodule TimelessMetrics.Call do
  @moduledoc false

  @read_timeout 10_000
  @write_timeout 30_000
  @maintenance_timeout 60_000

  def read(server, request), do: call(server, request, @read_timeout)
  def write(server, request), do: call(server, request, @write_timeout)
  def maintenance(server, request), do: call(server, request, @maintenance_timeout)

  def call(server, request, timeout) when is_integer(timeout) and timeout > 0 do
    GenServer.call(server, request, timeout)
  catch
    :exit, {:timeout, _details} -> timeout(server)
    :exit, {:noproc, _details} -> {:error, :unavailable}
    :exit, {:normal, _details} -> {:error, :unavailable}
    :exit, reason -> {:error, {:call_failed, reason}}
  end

  defp timeout(server) do
    if store = infer_store(server), do: TimelessMetrics.Stats.incr_timeouts(store)
    {:error, :timeout}
  end

  defp infer_store(server) when is_atom(server) do
    case Regex.run(
           ~r/^(.*)_(?:db|libsql_engine|libsql_reader_\d+|builder_\d+|shard_\d+|rollup|retention|registry)$/,
           Atom.to_string(server),
           capture: :all_but_first
         ) do
      [name] -> String.to_existing_atom(name)
      _ -> nil
    end
  rescue
    ArgumentError -> nil
  end

  defp infer_store(_server), do: nil
end
