defmodule TimelessMetrics.LabelMatchTest do
  use ExUnit.Case, async: true

  alias TimelessMetrics.LabelMatch

  test "libSQL planner encodes absent-label and negative semantics" do
    assert {%{"empty" => %{"re" => ""}, "env" => %{"neq" => "prod"}}, []} =
             LabelMatch.split_libsql_pushdown([
               {"empty", ""},
               {"env", {:not_equal, "prod"}}
             ])
  end

  test "libSQL planner pushes portable regexes and retains dialect-sensitive ones" do
    assert {%{"host" => %{"re" => "web-.*"}}, []} =
             LabelMatch.split_libsql_pushdown([{"host", {:regex, "web-.*"}}])

    assert {%{}, [{"host", {:regex, "."}}]} =
             LabelMatch.split_libsql_pushdown([{"host", {:regex, "."}}])

    assert {%{}, [{"host", {:regex, "(?=web).*"}}]} =
             LabelMatch.split_libsql_pushdown([{"host", {:regex, "(?=web).*"}}])

    assert {%{}, [{"host", {:regex, "web\\d+"}}]} =
             LabelMatch.split_libsql_pushdown([{"host", {:regex, "web\\d+"}}])
  end

  test "duplicate matchers use one safe candidate predicate and retain the full AND" do
    filter = [
      {"host", {:regex, "web-.*"}},
      {"host", {:not_equal, "web-2"}}
    ]

    assert {%{"host" => %{"re" => "web-.*"}}, ^filter} =
             LabelMatch.split_libsql_pushdown(filter)
  end

  test "invalid positive or negative regex makes the filter impossible" do
    assert :none = LabelMatch.split_libsql_pushdown([{"host", {:regex, "["}}])
    assert :none = LabelMatch.split_libsql_pushdown([{"host", {:not_regex, "["}}])
  end
end
