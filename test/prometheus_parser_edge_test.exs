defmodule TimelessMetrics.PrometheusParserEdgeTest do
  @moduledoc """
  Edge-case and fuzz tests for the Prometheus text parser.

  These began life as differential tests certifying the Rust parser against
  the C++ NIF it replaced (byte-identical output over adversarial input,
  mutation fuzz, and numeric fidelity corpora). The C++ oracle is gone; the
  fixed expectations remain as the parser's behavioral contract.
  """
  use ExUnit.Case, async: true

  alias TimelessMetrics.PrometheusNif

  # Labels are sorted so assertions are order-independent; duplicates are
  # preserved by sorting.
  defp parse_sorted(body) do
    {entries, errors} = PrometheusNif.parse(body)
    {Enum.map(entries, fn {n, l, v, t} -> {n, Enum.sort(l), v, t} end), errors}
  end

  describe "escape sequences in label values" do
    test "escaped quote inside value" do
      {[{"m", [{"k", value}], 1.0, 0}], 0} = parse_sorted(~S(m{k="a\"b"} 1.0) <> "\n")
      # Raw bytes are preserved (unescaping happens downstream)
      assert value == ~S(a\"b)
    end

    test "escaped backslash at end of value" do
      {[{"m", [{"k", ~S(a\\)}], 1.0, 0}], 0} = parse_sorted(~S(m{k="a\\"} 1.0) <> "\n")
    end

    test "escaped newline sequence in value" do
      {[{"m", [{"k", ~S(line1\nline2)}], 1.0, 0}], 0} =
        parse_sorted(~S(m{k="line1\nline2"} 1.0) <> "\n")
    end

    test "value containing comma and equals" do
      {[{"m", labels, 1.0, 0}], 0} = parse_sorted(~S(m{k="a,b=c",j="x"} 1.0) <> "\n")
      assert Enum.sort(labels) == [{"j", "x"}, {"k", "a,b=c"}]
    end
  end

  describe "malformed input" do
    test "unterminated label block is an error" do
      {[], 1} = parse_sorted(~s(m{k="v" 1.0\n))
    end

    test "unterminated quote consumes up to closing brace" do
      # The brace is located before label parsing starts, so the missing
      # quote truncates the value at '}' rather than failing the line.
      {[{"m", [{"k", "unclosed"}], 1.0, 0}], 0} = parse_sorted(~s(m{k="unclosed} 1.0\n))
    end

    test "missing value is an error" do
      {[], 1} = parse_sorted("just_a_name\n")
    end

    test "garbage value is an error" do
      {[], 1} = parse_sorted("m 1.0abc\n")
    end

    test "float overflow is an error" do
      {[], 1} = parse_sorted("m 1e400\n")
    end

    test "empty label key still parses" do
      parse_sorted(~s(m{="v"} 1.0\n))
    end

    test "label without quoted value stops label parsing" do
      parse_sorted("m{k=v} 1.0\n")
    end

    test "invalid UTF-8 bytes in label value are preserved" do
      {[{"m", [{"k", <<255, 254>>}], 1.0, 0}], 0} =
        parse_sorted(<<"m{k=\"", 255, 254, "\"} 1.0\n">>)
    end
  end

  describe "whitespace and line endings" do
    test "CRLF line endings" do
      {[{"a", [], 1.0, 0}, {"b", [], 2.0, 0}], 0} = parse_sorted("a 1.0\r\nb 2.0\r\n")
    end

    test "tabs as separators" do
      {[{"m", [], 1.0, 123}], 0} = parse_sorted("m\t1.0\t123\n")
    end

    test "multiple spaces between fields" do
      {[{"m", [], 1.0, 123}], 0} = parse_sorted("m   1.0   123\n")
    end

    test "leading whitespace before metric" do
      {[{"m", [], 1.0, 0}], 0} = parse_sorted("   m 1.0\n")
    end

    test "leading space before label key is tolerated" do
      {[{"m", [{"k", "v"}], 1.0, 0}], 0} = parse_sorted(~s(m{ k="v" } 1.0\n))
    end

    test "space after = silently drops the label block" do
      # Both parsers require '\"' immediately after '='; a space aborts
      # label parsing but the sample itself still parses.
      {[{"m", [], 1.0, 0}], 0} = parse_sorted(~s(m{ k = "v" } 1.0\n))
    end

    test "no trailing newline" do
      {[{"m", [], 1.0, 0}], 0} = parse_sorted("m 1.0")
    end

    test "whitespace-only lines are skipped silently" do
      {[{"m", [], 1.0, 0}], 0} = parse_sorted("   \n\t\nm 1.0\n  \n")
    end

    test "indented comment is skipped silently" do
      {[{"m", [], 1.0, 0}], 0} = parse_sorted("   # a comment\nm 1.0\n")
    end
  end

  describe "values and timestamps" do
    test "scientific notation" do
      {[{"m", [], 15_000_000_000.0, 0}], 0} = parse_sorted("m 1.5e10\n")
    end

    test "negative value and negative timestamp" do
      {[{"m", [], -42.5, -1000}], 0} = parse_sorted("m -42.5 -1000\n")
    end

    test "explicitly positive value" do
      {[{"m", [], 42.0, 0}], 0} = parse_sorted("m +42\n")
    end

    test "unparseable timestamp falls back to 0 sentinel" do
      {[{"m", [], 1.0, 0}], 0} = parse_sorted("m 1.0 12ab\n")
    end

    test "trailing junk after timestamp is ignored" do
      {[{"m", [], 1.0, 123}], 0} = parse_sorted("m 1.0 123 extra stuff\n")
    end

    test "very small and very large finite floats survive" do
      {[{"a", [], small, 0}, {"b", [], large, 0}], 0} =
        parse_sorted("a 2.2e-308\nb 1.7e308\n")

      assert small > 0.0
      assert large > 1.0e307
    end
  end

  describe "labels" do
    test "duplicate label keys are both preserved" do
      {[{"m", labels, 1.0, 0}], 0} = parse_sorted(~s(m{k="1",k="2"} 1.0\n))
      assert length(labels) == 2
    end

    test "empty label value" do
      {[{"m", [{"k", ""}], 1.0, 0}], 0} = parse_sorted(~s(m{k=""} 1.0\n))
    end

    test "UTF-8 label values" do
      {[{"m", [{"k", "héllo wörld ✓"}], 1.0, 0}], 0} =
        parse_sorted(~s(m{k="héllo wörld ✓"} 1.0\n))
    end

    test "many labels on one line" do
      labels = Enum.map_join(1..30, ",", fn i -> ~s(k#{i}="v#{i}") end)
      {[{"m", parsed, 1.0, 0}], 0} = parse_sorted("m{#{labels}} 1.0\n")
      assert length(parsed) == 30
    end
  end

  describe "adversarial structure" do
    test "EOF in the middle of an escape sequence, no closing brace" do
      {[], 1} = parse_sorted("m{k=\"v\\")
    end

    test "double label block fails on the second block" do
      {[], 1} = parse_sorted(~s(m{a="1"}{b="2"} 1\n))
    end

    test "line starting with a brace has no metric name" do
      {[], 1} = parse_sorted(~s({k="v"} 1\n))
    end

    test "escaped quote just before closing brace stays raw" do
      # Bytes: m{k="a\"} 1 — value is a\" (brace wins over the escape)
      {[{"m", [{"k", "a\\\""}], 1.0, 0}], 0} = parse_sorted("m{k=\"a\\\"} 1\n")
    end

    test "null byte lands in the metric name" do
      {[{<<109, 0>>, [], 1.0, 0}], 0} = parse_sorted(<<109, 0, " 1.0\n">>)
    end

    test "bare hash is a silent comment" do
      {[], 0} = parse_sorted("#\n#\n")
    end

    test "lone punctuation values are errors" do
      for v <- ["-", "+", ".", "e5", "--1", "1.2.3"] do
        {[], 1} = parse_sorted("m #{v}\n")
      end
    end

    test "1MB metric name on a single unterminated line" do
      name = String.duplicate("a", 1_048_576)
      {[{^name, [], 1.0, 0}], 0} = parse_sorted(name <> " 1.0")
    end

    test "64KB label value" do
      big = String.duplicate("z", 65_536)
      {[{"m", [{"k", ^big}], 1.0, 0}], 0} = parse_sorted(~s(m{k="#{big}"} 1.0\n))
    end

    test "10k labels on one line" do
      labels = Enum.map_join(1..10_000, ",", fn i -> ~s(k#{i}="v#{i}") end)
      {[{"m", parsed, 1.0, 0}], 0} = parse_sorted("m{#{labels}} 1.0\n")
      assert length(parsed) == 10_000
    end

    test "deeply stacked backslashes" do
      # Bytes: m{k="\\\\\\"} 1 — six backslashes pair up into three
      # escapes, so the following quote is a real (unescaped) closer.
      {[{"m", [{"k", value}], 1.0, 0}], 0} = parse_sorted("m{k=\"\\\\\\\\\\\\\"} 1\n")
      assert value == "\\\\\\\\\\\\"
    end
  end

  describe "strictness contract (deliberate differences from the removed C++ parser)" do
    # The old strtod/strtoll-based C++ parser accepted artifacts that are
    # not valid Prometheus exposition format. These pin the stricter
    # behavior of the Rust parser as the contract.

    test "hex floats are rejected (strtod accepted them)" do
      assert {[], 1} = parse_sorted("m 0x10\n")
    end

    test "timestamp overflow uses the 0 sentinel (strtoll saturated to i64 max)" do
      assert {[{"m", [], 1.0, 0}], 0} = parse_sorted("m 1.0 99999999999999999999\n")
    end

    test "numeric fields >= 64 chars parse (C++ had a fixed-buffer limit)" do
      body = "m " <> String.duplicate("0", 80) <> "5\n"
      assert {[{"m", [], 5.0, 0}], 0} = parse_sorted(body)
    end
  end

  describe "numeric fidelity" do
    test "1000 random floats round-trip bit-identically" do
      :rand.seed(:exsss, {42, 43, 44})

      floats =
        for _ <- 1..1000 do
          mantissa = :rand.uniform() * 2.0 - 1.0
          exponent = :rand.uniform(600) - 300
          mantissa * 10.0 ** exponent
        end

      body =
        Enum.map_join(floats, "\n", fn f ->
          "m #{:erlang.float_to_binary(f, [:short])}"
        end) <> "\n"

      {entries, 0} = parse_sorted(body)
      parsed = Enum.map(entries, fn {_, _, v, _} -> v end)

      # Shortest-repr round trip through Rust's f64 parse must be exact.
      assert parsed == floats
    end
  end

  describe "byte-mutation fuzz" do
    # Substitution/truncation mutations over valid bodies: the parser must
    # never crash and must always return well-formed {entries, errors}.
    @fuzz_alphabet ~c({}"=\\,# \t.0123456789abkmve-+E)

    test "2000 mutated bodies never crash the parser" do
      :rand.seed(:exsss, {7, 8, 9})

      for round <- 1..2000 do
        body = mutate(base_body(round))

        try do
          {entries, errors} = parse_sorted(body)
          assert is_list(entries)
          assert is_integer(errors) and errors >= 0
        rescue
          e in ExUnit.AssertionError -> reraise e, __STACKTRACE__
          e -> flunk("parser crashed on #{inspect(body)}: #{Exception.message(e)}")
        end
      end
    end

    defp base_body(round) do
      Enum.map_join(1..10, "\n", fn i ->
        case rem(round + i, 4) do
          0 -> ~s(metric_#{i}{host="h#{i}",dc="d#{i}"} #{i}.5 170000000000#{i})
          1 -> "metric_#{i} #{i}.25"
          2 -> ~s(m#{i}{k="a\\"b",j="#{i}"} -#{i}e2)
          3 -> "# help text #{i}"
        end
      end) <> "\n"
    end

    defp mutate(body) do
      bytes = :binary.bin_to_list(body)
      n_mutations = :rand.uniform(8)

      mutated =
        Enum.reduce(1..n_mutations, bytes, fn _, acc ->
          pos = :rand.uniform(length(acc)) - 1
          char = Enum.at(@fuzz_alphabet, :rand.uniform(length(@fuzz_alphabet)) - 1)
          List.replace_at(acc, pos, char)
        end)

      # Occasionally truncate mid-line
      mutated =
        if :rand.uniform(4) == 1 do
          Enum.take(mutated, :rand.uniform(max(length(mutated), 1)))
        else
          mutated
        end

      :binary.list_to_bin(mutated)
    end
  end

  describe "randomized corpus" do
    test "500 mixed valid/invalid generated lines parse with expected mix" do
      :rand.seed(:exsss, {101, 102, 103})

      body =
        Enum.map_join(1..500, "\n", fn i -> gen_line(i, :rand.uniform(12)) end) <> "\n"

      {entries, errors} = parse_sorted(body)
      # Sanity: the corpus must actually exercise both paths
      assert length(entries) > 100
      assert errors > 10
    end

    defp gen_line(i, roll) do
      name = "metric_#{rem(i, 17)}"
      labels = ~s(host="h#{rem(i, 5)}",dc="d#{rem(i, 3)}")

      case roll do
        1 -> "#{name} #{i}.5"
        2 -> "#{name}{#{labels}} #{i}.25 #{1_700_000_000_000 + i}"
        3 -> "# comment #{i}"
        4 -> ""
        5 -> "#{name}{#{labels}} NaN"
        6 -> "#{name} not_a_number"
        7 -> ~s(#{name}{k="esc\\"#{i}"} #{i})
        8 -> "#{name}{unclosed #{i}"
        9 -> "\t#{name}\t#{i}e2\t#{i}"
        10 -> "#{name}{} #{i} garbage_ts"
        11 -> String.duplicate(" ", rem(i, 4)) <> "#{name} -#{i}.125 -#{i}"
        12 -> "#{name}{#{labels},x="
      end
    end
  end
end
