defmodule TimelessMetrics.RustEngineTest do
  use ExUnit.Case, async: true

  alias TimelessMetrics.RustEngine

  describe "normalize_nif_result/1" do
    test "accepts wrapped rustler result payloads" do
      assert RustEngine.normalize_nif_result({:ok, {:ok, 123}}) == {:ok, 123}
      assert RustEngine.normalize_nif_result({:ok, %{}}) == {:ok, %{}}
    end

    test "accepts bare success payloads from older NIF artifacts" do
      assert RustEngine.normalize_nif_result(:ok) == {:ok, :ok}

      assert RustEngine.normalize_nif_result(%{"series_count" => 1}) ==
               {:ok, %{"series_count" => 1}}
    end

    test "passes through errors" do
      assert RustEngine.normalize_nif_result({:error, "bad write"}) == {:error, "bad write"}
    end
  end
end
