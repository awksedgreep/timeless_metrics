defmodule TimelessMetrics.RuntimeConfigTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  test "invalid integer and boolean environment values warn and use defaults" do
    old_port = System.get_env("TIMELESS_PORT")
    old_auto_migrate = System.get_env("TIMELESS_AUTO_MIGRATE")

    on_exit(fn ->
      restore_env("TIMELESS_PORT", old_port)
      restore_env("TIMELESS_AUTO_MIGRATE", old_auto_migrate)
    end)

    System.put_env("TIMELESS_PORT", "not-a-port")
    System.put_env("TIMELESS_AUTO_MIGRATE", "sometimes")
    parent = self()

    warnings =
      capture_io(:stderr, fn ->
        config = Config.Reader.read!("config/runtime.exs", env: :prod)
        send(parent, {:runtime_config, config[:timeless_metrics]})
      end)

    assert_receive {:runtime_config, config}
    assert config[:port] == 8428
    assert config[:auto_migrate] == true
    assert warnings =~ "ignoring invalid TIMELESS_PORT"
    assert warnings =~ "ignoring invalid TIMELESS_AUTO_MIGRATE"
  end

  defp restore_env(name, nil), do: System.delete_env(name)
  defp restore_env(name, value), do: System.put_env(name, value)
end
