defmodule TimelessMetrics.EngineVocabularyTest do
  @moduledoc """
  The engine option refuses values it does not understand.

  The three Timeless packages spell their previous-generation engine
  differently — `:rust` here, `:elixir` in timeless_logs and timeless_traces —
  and they used to resolve an unrecognised value in opposite directions: this
  supervisor fell through to libSQL, while the other two fell through to their
  legacy engine. So the same mistake silently upgraded one signal and silently
  downgraded another.

  The sharp edge was `engine: :elixir` here. It is a real engine name in the
  sibling packages, it meant "keep the old engine", and it landed on libSQL —
  which then converted the store, because `auto_migrate` defaults to true. An
  operator asking for the legacy engine got an automatic migration instead.
  """

  use ExUnit.Case, async: false

  defp start(engine) do
    dir = Path.join(System.tmp_dir!(), "tm_vocab_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)

    TimelessMetrics.Supervisor.start_link(
      name: :"vocab_#{System.unique_integer([:positive])}",
      engine: engine,
      data_dir: dir,
      scraping: false,
      self_monitor: false,
      reader_pool_size: 1
    )
  end

  test "the sibling packages' legacy engine name is refused, not silently upgraded" do
    Process.flag(:trap_exit, true)

    assert {:error, {%ArgumentError{message: message}, _}} = start(:elixir)

    assert message =~ ":elixir"
    # The error has to name the value that actually works, since the whole
    # failure mode is an operator carrying vocabulary between packages.
    assert message =~ ":rust"
  end

  test "an unrecognised engine is refused rather than resolved to a default" do
    Process.flag(:trap_exit, true)

    assert {:error, {%ArgumentError{message: message}, _}} = start(:libsq1)
    assert message =~ "invalid"
  end

  test "the supported engines still start" do
    assert {:ok, _} = start(:libsql)
    assert {:ok, _} = start(:rust)
  end
end
