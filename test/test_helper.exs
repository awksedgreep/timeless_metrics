ExUnit.start()

defmodule TimelessMetrics.TestHelper do
  @doc """
  Ensure a previously-registered supervisor is fully stopped before restarting.
  Works around an OTP 28 race where ExUnit's stop_supervised returns before
  the supervisor process is fully terminated.
  """
  def await_down(name) do
    case Process.whereis(name) do
      nil ->
        :ok

      pid ->
        ref = Process.monitor(pid)
        Supervisor.stop(pid, :shutdown)

        receive do
          {:DOWN, ^ref, _, _, _} -> :ok
        after
          10_000 -> :ok
        end
    end
  end
end
