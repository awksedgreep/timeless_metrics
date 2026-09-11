defmodule TimelessMetrics.DBTest do
  use ExUnit.Case, async: true

  test "execute returns tagged errors instead of raising for invalid SQL" do
    {:ok, conn} = Exqlite.Sqlite3.open(":memory:")
    on_exit(fn -> Exqlite.Sqlite3.close(conn) end)

    assert {:error, {:sqlite, _reason, "NOT VALID SQL"}} =
             TimelessMetrics.DB.execute(conn, "NOT VALID SQL", [])
  end
end
