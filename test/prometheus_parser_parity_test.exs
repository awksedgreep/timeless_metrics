defmodule TimelessMetrics.PrometheusParserParityTest do
  @moduledoc """
  Differential tests: the Rust parser (RustEngine.Nif.parse_prometheus_terms)
  must produce output identical to the C++ parser (PrometheusNif.parse) for
  every input, plus explicit edge-case assertions for behaviors we rely on.

  The C++ parser acts as the oracle certifying its Rust replacement. Once the
  C++ NIF is removed, the parity tests convert to fixed expected-output tests
  (the `expected` values are asserted independently of both parsers).
  """
  use ExUnit.Case, async: true

  alias TimelessMetrics.PrometheusNif
  alias TimelessMetrics.RustEngine.Nif

  # Label order differs between parsers (C++ builds proplists in reverse).
  # Sort labels for comparison; duplicates are preserved by sorting.
  defp normalize({entries, errors}) do
    {Enum.map(entries, fn {n, l, v, t} -> {n, Enum.sort(l), v, t} end), errors}
  end

  defp parse_both(body) do
    cpp = normalize(PrometheusNif.parse(body))
    rust = normalize(Nif.parse_prometheus_terms(body))
    assert cpp == rust, "parser divergence on: #{inspect(body)}"
    cpp
  end

  describe "escape sequences in label values" do
    test "escaped quote inside value" do
      {[{"m", [{"k", value}], 1.0, 0}], 0} = parse_both(~S(m{k="a\"b"} 1.0) <> "\n")
      # Raw bytes are preserved (unescaping happens downstream, as in C++)
      assert value == ~S(a\"b)
    end

    test "escaped backslash at end of value" do
      {[{"m", [{"k", ~S(a\\)}], 1.0, 0}], 0} = parse_both(~S(m{k="a\\"} 1.0) <> "\n")
    end

    test "escaped newline sequence in value" do
      {[{"m", [{"k", ~S(line1\nline2)}], 1.0, 0}], 0} =
        parse_both(~S(m{k="line1\nline2"} 1.0) <> "\n")
    end

    test "value containing comma and equals" do
      {[{"m", labels, 1.0, 0}], 0} = parse_both(~S(m{k="a,b=c",j="x"} 1.0) <> "\n")
      assert Enum.sort(labels) == [{"j", "x"}, {"k", "a,b=c"}]
    end
  end

  describe "malformed input" do
    test "unterminated label block is an error" do
      {[], 1} = parse_both(~s(m{k="v" 1.0\n))
    end

    test "unterminated quote consumes up to closing brace" do
      # The brace is located before label parsing starts, so the missing
      # quote truncates the value at '}' rather than failing the line.
      {[{"m", [{"k", "unclosed"}], 1.0, 0}], 0} = parse_both(~s(m{k="unclosed} 1.0\n))
    end

    test "missing value is an error" do
      {[], 1} = parse_both("just_a_name\n")
    end

    test "garbage value is an error" do
      {[], 1} = parse_both("m 1.0abc\n")
    end

    test "float overflow is an error" do
      {[], 1} = parse_both("m 1e400\n")
    end

    test "empty label key still parses" do
      parse_both(~s(m{="v"} 1.0\n))
    end

    test "label without quoted value stops label parsing" do
      parse_both("m{k=v} 1.0\n")
    end

    test "invalid UTF-8 bytes in label value are preserved" do
      {[{"m", [{"k", <<255, 254>>}], 1.0, 0}], 0} =
        parse_both(<<"m{k=\"", 255, 254, "\"} 1.0\n">>)
    end
  end

  describe "whitespace and line endings" do
    test "CRLF line endings" do
      {[{"a", [], 1.0, 0}, {"b", [], 2.0, 0}], 0} = parse_both("a 1.0\r\nb 2.0\r\n")
    end

    test "tabs as separators" do
      {[{"m", [], 1.0, 123}], 0} = parse_both("m\t1.0\t123\n")
    end

    test "multiple spaces between fields" do
      {[{"m", [], 1.0, 123}], 0} = parse_both("m   1.0   123\n")
    end

    test "leading whitespace before metric" do
      {[{"m", [], 1.0, 0}], 0} = parse_both("   m 1.0\n")
    end

    test "leading space before label key is tolerated" do
      {[{"m", [{"k", "v"}], 1.0, 0}], 0} = parse_both(~s(m{ k="v" } 1.0\n))
    end

    test "space after = silently drops the label block" do
      # Both parsers require '\"' immediately after '='; a space aborts
      # label parsing but the sample itself still parses.
      {[{"m", [], 1.0, 0}], 0} = parse_both(~s(m{ k = "v" } 1.0\n))
    end

    test "no trailing newline" do
      {[{"m", [], 1.0, 0}], 0} = parse_both("m 1.0")
    end

    test "whitespace-only lines are skipped silently" do
      {[{"m", [], 1.0, 0}], 0} = parse_both("   \n\t\nm 1.0\n  \n")
    end

    test "indented comment is skipped silently" do
      {[{"m", [], 1.0, 0}], 0} = parse_both("   # a comment\nm 1.0\n")
    end
  end

  describe "values and timestamps" do
    test "scientific notation" do
      {[{"m", [], 15_000_000_000.0, 0}], 0} = parse_both("m 1.5e10\n")
    end

    test "negative value and negative timestamp" do
      {[{"m", [], -42.5, -1000}], 0} = parse_both("m -42.5 -1000\n")
    end

    test "explicitly positive value" do
      {[{"m", [], 42.0, 0}], 0} = parse_both("m +42\n")
    end

    test "unparseable timestamp falls back to 0 sentinel" do
      {[{"m", [], 1.0, 0}], 0} = parse_both("m 1.0 12ab\n")
    end

    test "trailing junk after timestamp is ignored" do
      {[{"m", [], 1.0, 123}], 0} = parse_both("m 1.0 123 extra stuff\n")
    end

    test "very small and very large finite floats survive" do
      {[{"a", [], small, 0}, {"b", [], large, 0}], 0} =
        parse_both("a 2.2e-308\nb 1.7e308\n")

      assert small > 0.0
      assert large > 1.0e307
    end
  end

  describe "labels" do
    test "duplicate label keys are both preserved" do
      {[{"m", labels, 1.0, 0}], 0} = parse_both(~s(m{k="1",k="2"} 1.0\n))
      assert length(labels) == 2
    end

    test "empty label value" do
      {[{"m", [{"k", ""}], 1.0, 0}], 0} = parse_both(~s(m{k=""} 1.0\n))
    end

    test "UTF-8 label values" do
      {[{"m", [{"k", "héllo wörld ✓"}], 1.0, 0}], 0} =
        parse_both(~s(m{k="héllo wörld ✓"} 1.0\n))
    end

    test "many labels on one line" do
      labels = Enum.map_join(1..30, ",", fn i -> ~s(k#{i}="v#{i}") end)
      {[{"m", parsed, 1.0, 0}], 0} = parse_both("m{#{labels}} 1.0\n")
      assert length(parsed) == 30
    end
  end

  describe "adversarial structure" do
    test "EOF in the middle of an escape sequence, no closing brace" do
      {[], 1} = parse_both("m{k=\"v\\")
    end

    test "double label block fails on the second block" do
      {[], 1} = parse_both(~s(m{a="1"}{b="2"} 1\n))
    end

    test "line starting with a brace has no metric name" do
      {[], 1} = parse_both(~s({k="v"} 1\n))
    end

    test "escaped quote just before closing brace stays raw" do
      # Bytes: m{k="a\"} 1 — value is a\" (brace wins over the escape)
      {[{"m", [{"k", "a\\\""}], 1.0, 0}], 0} = parse_both("m{k=\"a\\\"} 1\n")
    end

    test "null byte lands in the metric name" do
      {[{<<109, 0>>, [], 1.0, 0}], 0} = parse_both(<<109, 0, " 1.0\n">>)
    end

    test "bare hash is a silent comment" do
      {[], 0} = parse_both("#\n#\n")
    end

    test "lone punctuation values are errors" do
      for v <- ["-", "+", ".", "e5", "--1", "1.2.3"] do
        {[], 1} = parse_both("m #{v}\n")
      end
    end

    test "1MB metric name on a single unterminated line" do
      name = String.duplicate("a", 1_048_576)
      {[{^name, [], 1.0, 0}], 0} = parse_both(name <> " 1.0")
    end

    test "64KB label value" do
      big = String.duplicate("z", 65_536)
      {[{"m", [{"k", ^big}], 1.0, 0}], 0} = parse_both(~s(m{k="#{big}"} 1.0\n))
    end

    test "10k labels on one line" do
      labels = Enum.map_join(1..10_000, ",", fn i -> ~s(k#{i}="v#{i}") end)
      {[{"m", parsed, 1.0, 0}], 0} = parse_both("m{#{labels}} 1.0\n")
      assert length(parsed) == 10_000
    end

    test "deeply stacked backslashes" do
      # Bytes: m{k="\\\\\\"} 1 — six backslashes pair up into three
      # escapes, so the following quote is a real (unescaped) closer.
      {[{"m", [{"k", value}], 1.0, 0}], 0} = parse_both("m{k=\"\\\\\\\\\\\\\"} 1\n")
      assert value == "\\\\\\\\\\\\"
    end
  end

  describe "known intentional divergences (Rust behavior is the contract)" do
    # These are cases where C's strtod/strtoll accept artifacts that are not
    # valid Prometheus exposition format. The Rust parser is stricter; after
    # the C++ NIF is removed these tests keep asserting the Rust behavior.

    test "hex floats: C++ accepts via strtod, Rust rejects" do
      body = "m 0x10\n"
      assert {[{"m", [], 16.0, 0}], 0} = PrometheusNif.parse(body)
      assert {[], 1} = Nif.parse_prometheus_terms(body)
    end

    test "timestamp overflow: C++ saturates to i64 max, Rust uses 0 sentinel" do
      body = "m 1.0 99999999999999999999\n"
      assert {[{"m", [], 1.0, 9_223_372_036_854_775_807}], 0} = PrometheusNif.parse(body)
      assert {[{"m", [], 1.0, 0}], 0} = Nif.parse_prometheus_terms(body)
    end

    test "numeric fields >= 64 chars: C++ rejects (fixed buffer), Rust parses" do
      body = "m " <> String.duplicate("0", 80) <> "5\n"
      assert {[], 1} = PrometheusNif.parse(body)
      assert {[{"m", [], 5.0, 0}], 0} = Nif.parse_prometheus_terms(body)
    end
  end

  describe "numeric fidelity" do
    test "1000 random floats parse bit-identically in both parsers" do
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

      {entries, 0} = parse_both(body)
      parsed = Enum.map(entries, fn {_, _, v, _} -> v end)

      # Shortest-repr round trip must be exact — and both parsers agreed
      # byte-for-byte via parse_both, so C++ strtod == Rust f64::parse here.
      assert parsed == floats
    end
  end

  describe "byte-mutation fuzz" do
    # Substitution/truncation mutations over valid bodies. The alphabet
    # avoids x/X (hex-float divergence) and mutations never insert bytes,
    # so digit runs cannot grow into the i64-overflow divergence.
    @fuzz_alphabet ~c({}"=\\,# \t.0123456789abkmve-+E)

    test "2000 mutated bodies never diverge or crash" do
      :rand.seed(:exsss, {7, 8, 9})

      for round <- 1..2000 do
        body = mutate(base_body(round))

        try do
          parse_both(body)
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

  describe "randomized differential corpus" do
    test "parsers agree on 500 mixed valid/invalid generated lines" do
      :rand.seed(:exsss, {101, 102, 103})

      body =
        Enum.map_join(1..500, "\n", fn i -> gen_line(i, :rand.uniform(12)) end) <> "\n"

      {entries, errors} = parse_both(body)
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
