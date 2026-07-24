defmodule TimelessMetrics.PromQL do
  @moduledoc """
  PromQL parser and evaluator for the Prometheus/VictoriaMetrics-compatible API.

  A tokenizer + recursive-descent parser produces an AST; the evaluator
  fetches raw samples and evaluates Prometheus/VM semantics on an exact
  `start + n*step` grid — instant selectors take the most recent sample
  within the lookback window, range functions slide their `[window]` over
  raw samples, and counter functions (`rate`/`increase`/`irate`) are
  reset-adjusted with carry-in from before the window.

  ## Supported

    * Selectors: `m`, `m{a="x",b=~"y",c!="z",d!~"w"}`, `{__name__=~"cpu_.*"}`
    * Range functions: `rate`, `irate`, `increase`, `avg/min/max/sum/count_over_time`,
      `last_over_time`, `first_over_time`
    * Aggregations: `sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`,
      `group`, `quantile(q, ...)`, `topk(k, ...)`, `bottomk(k, ...)` — with
      `by (...)` / `without (...)` in prefix or suffix position, or bare
      (collapses to a single series, dropping `__name__`)
    * Binary operators: `+ - * / % ^`, comparisons (`> < >= <= == !=`, with
      `bool`), and set operators `and` / `or` / `unless`; scalar–vector and
      1:1 vector–vector matching
    * Value transforms: `abs`, `ceil`, `floor`, `round`, `sqrt`, `exp`, `ln`,
      `log2`, `log10`, `clamp`, `clamp_min`, `clamp_max`
    * `offset` modifier

  Anything unsupported returns `{:error, reason}` — never a silent empty
  success — so API clients get a Prometheus-style error response.
  """

  @keywords ~w(by without offset bool and or unless on ignoring group_left group_right)

  @agg_ops ~w(sum min max avg count stddev stdvar topk bottomk quantile group count_values)

  @rollup_fns [
    :rate,
    :irate,
    :increase,
    :avg_over_time,
    :min_over_time,
    :max_over_time,
    :sum_over_time,
    :count_over_time,
    :last_over_time,
    :first_over_time,
    :delta,
    :idelta,
    :deriv,
    :changes,
    :resets,
    :present_over_time,
    :stddev_over_time,
    :stdvar_over_time
  ]
  # VM name policy (verified against a real VM instance via scripts/vm_diff.exs):
  # rollups whose result is "the same quantity" keep the metric name; rate-like
  # and sum/count rollups drop it. Note this differs from strict Prometheus,
  # which drops the name on all of these — VM is our compatibility target.
  @name_keeping_rollups [
    :last_over_time,
    :first_over_time,
    :avg_over_time,
    :min_over_time,
    :max_over_time
  ]

  @transform_fns [
    :abs,
    :ceil,
    :floor,
    :round,
    :sqrt,
    :exp,
    :ln,
    :log2,
    :log10,
    :sgn,
    :acos,
    :acosh,
    :asin,
    :asinh,
    :atan,
    :atanh,
    :cos,
    :cosh,
    :sin,
    :sinh,
    :tan,
    :tanh,
    :deg,
    :rad
  ]

  # Same policy for transforms: ceil/floor/round/clamp* keep the name in VM;
  # abs/sqrt/exp/log* drop it.
  @name_keeping_transforms [:ceil, :floor, :round]

  @functions @rollup_fns ++
               @transform_fns ++
               [
                 :clamp,
                 :clamp_min,
                 :clamp_max,
                 :pi,
                 :predict_linear,
                 :quantile_over_time,
                 :label_replace,
                 :label_join,
                 :histogram_quantile
               ]

  # Recognized PromQL functions we don't implement yet — named in the error
  # message so clients can tell "unsupported" from "typo".
  @known_unsupported ~w(histogram_fraction histogram_avg histogram_count histogram_sum
    histogram_stddev histogram_stdvar
    sort sort_desc sort_by_label sort_by_label_desc
    scalar vector absent absent_over_time
    time timestamp mad_over_time
    ceil_over_time floor_over_time
    double_exponential_smoothing holt_winters
    day_of_month day_of_week day_of_year days_in_month
    hour minute month year
    limitk limit_ratio info)

  # VictoriaMetrics MetricsQL extensions — distinct message so VM users know
  # the construct exists but is VM-specific.
  @metricsql_fns ~w(default_rollup label_set label_del label_keep label_map
    alias union range_avg range_max range_min range_sum running_avg running_sum
    quantiles distinct increase_pure remove_resets interpolate keep_last_value
    keep_next_value drop_common_labels rate_over_sum with)

  # MetricsQL infix/suffix operators that appear after a complete PromQL
  # expression (e.g. `expr default 0`, `expr keep_metric_names`).
  @metricsql_operators ~w(default if ifnot keep_metric_names)

  # ===== Public API =====

  @doc """
  Parse a PromQL query string into an AST.

  Returns `{:ok, ast}` or `{:error, reason}`. Invalid or unsupported syntax is
  always an error — a query never silently degrades to a metric-name lookup.
  """
  def parse(query) when is_binary(query) do
    trimmed = String.trim(query)

    if trimmed == "" do
      {:error, "empty query"}
    else
      with {:ok, tokens} <- tokenize(trimmed),
           {:ok, node, rest} <- parse_expr(tokens) do
        case rest do
          [] ->
            {:ok, node}

          [{:ident, name} | _] when name in @metricsql_operators ->
            {:error,
             "'#{name}' is a VictoriaMetrics MetricsQL operator and is not supported (standard PromQL only)"}

          [t | _] ->
            {:error, "unexpected #{describe_token(t)} after complete expression"}
        end
      end
    end
  end

  # Prometheus enforces the same cap; Grafana relies on the error to re-query
  # at a coarser step.
  @max_points_per_series 11_000

  @doc """
  Evaluate a parsed AST over a time range against a store.

  Evaluation follows Prometheus/VM semantics: every series is evaluated at
  the exact grid `start, start+step, ..., end`. Instant selectors take the
  most recent sample within the lookback window (default 300s, configure
  with `config :timeless_metrics, promql_lookback_seconds: n`); range
  functions evaluate over their `[window]` of raw samples ending at each
  grid point.

  Returns `{:ok, prometheus_matrix_response}` or `{:error, reason}`.
  """
  def execute(ast, store, start_ts, end_ts, step) do
    ctx = %{
      store: store,
      from: start_ts,
      to: end_ts,
      step: step,
      lookback: Application.get_env(:timeless_metrics, :promql_lookback_seconds, 300)
    }

    grid_points = div(end_ts - start_ts, step) + 1

    cond do
      end_ts < start_ts ->
        {:error, "end timestamp must not be before start timestamp"}

      grid_points > @max_points_per_series ->
        {:error,
         "exceeded maximum resolution of #{@max_points_per_series} points per timeseries — decrease the query resolution (increase step)"}

      true ->
        case eval(ast, ctx) do
          {:ok, {:vector, series}} ->
            {:ok, wrap_prom_response(format_series(series))}

          {:ok, {:scalar, n}} ->
            values = for ts <- start_ts..end_ts//step, do: [ts, format_value(n)]
            {:ok, wrap_prom_response([%{"metric" => %{}, "values" => values}])}

          {:error, msg} ->
            {:error, msg}
        end
    end
  end

  @doc """
  Extract the first vector selector from an AST as
  `%{metric: name | nil, metric_pattern: regex | nil, labels: map}`.

  Used by the `/api/v1/series` endpoint, which only needs to know which
  series a match[] expression touches.
  """
  def selector_info(ast) do
    case find_selector(ast) do
      %{metric: m, pattern: p, labels: l} -> %{metric: m, metric_pattern: p, labels: l}
      nil -> %{metric: nil, metric_pattern: nil, labels: %{}}
    end
  end

  defp find_selector({:selector, sel}), do: sel
  defp find_selector({:range, node, _}), do: find_selector(node)
  defp find_selector({:call, _, args}), do: Enum.find_value(args, &find_selector/1)
  defp find_selector({:agg, _, _, _, expr}), do: find_selector(expr)
  defp find_selector({:binop, _, _, l, r}), do: find_selector(l) || find_selector(r)
  defp find_selector(_), do: nil

  # ===== Tokenizer =====
  #
  # Tokens:
  #   {:ident, name} {:kw, atom} {:number, float} {:duration, seconds}
  #   {:string, s} {:op, atom} :lparen :rparen :lbrace :rbrace
  #   :lbracket :rbracket :comma :at

  defp tokenize(bin), do: do_tokenize(bin, [])

  defp do_tokenize(<<>>, acc), do: {:ok, Enum.reverse(acc)}

  defp do_tokenize(<<c, rest::binary>>, acc) when c in ~c" \t\n\r",
    do: do_tokenize(rest, acc)

  defp do_tokenize(<<"==", rest::binary>>, acc), do: do_tokenize(rest, [{:op, :eq} | acc])
  defp do_tokenize(<<"!=", rest::binary>>, acc), do: do_tokenize(rest, [{:op, :neq} | acc])
  defp do_tokenize(<<">=", rest::binary>>, acc), do: do_tokenize(rest, [{:op, :gte} | acc])
  defp do_tokenize(<<"<=", rest::binary>>, acc), do: do_tokenize(rest, [{:op, :lte} | acc])
  defp do_tokenize(<<"=~", rest::binary>>, acc), do: do_tokenize(rest, [{:op, :re} | acc])
  defp do_tokenize(<<"!~", rest::binary>>, acc), do: do_tokenize(rest, [{:op, :nre} | acc])

  defp do_tokenize(<<q, rest::binary>>, acc) when q in [?", ?'] do
    case lex_string(rest, q, []) do
      {:ok, s, rest2} -> do_tokenize(rest2, [{:string, s} | acc])
      {:error, _} = err -> err
    end
  end

  defp do_tokenize(<<c, _::binary>> = bin, acc) when c in ?0..?9 or c == ?. do
    case lex_number(bin) do
      {:ok, token, rest} -> do_tokenize(rest, [token | acc])
      {:error, _} = err -> err
    end
  end

  defp do_tokenize(<<c, _::binary>> = bin, acc)
       when c in ?a..?z or c in ?A..?Z or c == ?_ or c == ?: do
    {name, rest} = lex_ident(bin, [])

    token =
      if name in @keywords, do: {:kw, String.to_existing_atom(name)}, else: {:ident, name}

    do_tokenize(rest, [token | acc])
  end

  defp do_tokenize(<<c, rest::binary>>, acc) when c in ~c"+-*/%^><=(){}[],@" do
    do_tokenize(rest, [single_char_token(c) | acc])
  end

  defp do_tokenize(<<c, _::binary>>, _acc), do: {:error, "unexpected character: #{<<c>>}"}

  defp single_char_token(?+), do: {:op, :add}
  defp single_char_token(?-), do: {:op, :sub}
  defp single_char_token(?*), do: {:op, :mul}
  defp single_char_token(?/), do: {:op, :div}
  defp single_char_token(?%), do: {:op, :mod}
  defp single_char_token(?^), do: {:op, :pow}
  defp single_char_token(?>), do: {:op, :gt}
  defp single_char_token(?<), do: {:op, :lt}
  defp single_char_token(?=), do: {:op, :assign}
  defp single_char_token(?(), do: :lparen
  defp single_char_token(?)), do: :rparen
  defp single_char_token(?{), do: :lbrace
  defp single_char_token(?}), do: :rbrace
  defp single_char_token(?[), do: :lbracket
  defp single_char_token(?]), do: :rbracket
  defp single_char_token(?,), do: :comma
  defp single_char_token(?@), do: :at

  defp lex_string(<<>>, _q, _acc), do: {:error, "unterminated string literal"}

  defp lex_string(<<?\\, c, rest::binary>>, q, acc),
    do: lex_string(rest, q, [unescape(c) | acc])

  defp lex_string(<<q, rest::binary>>, q, acc),
    do: {:ok, acc |> Enum.reverse() |> List.to_string(), rest}

  defp lex_string(<<c, rest::binary>>, q, acc), do: lex_string(rest, q, [c | acc])

  defp unescape(?n), do: ?\n
  defp unescape(?t), do: ?\t
  defp unescape(c), do: c

  @duration_re ~r/^(?:\d+(?:ms|[smhdwy]))+/
  @number_re ~r/^(?:\d+\.\d+|\d+|\.\d+)(?:[eE][+-]?\d+)?/

  defp lex_number(bin) do
    case Regex.run(@duration_re, bin) do
      [full] ->
        {:ok, {:duration, duration_seconds(full)}, chop(bin, full)}

      nil ->
        case Regex.run(@number_re, bin) do
          [full] -> {:ok, {:number, number_value(full)}, chop(bin, full)}
          nil -> {:error, "invalid number"}
        end
    end
  end

  defp chop(bin, prefix) do
    binary_part(bin, byte_size(prefix), byte_size(bin) - byte_size(prefix))
  end

  defp number_value(s) do
    s = if String.starts_with?(s, "."), do: "0" <> s, else: s
    s = String.replace(s, "E", "e")
    s = if String.contains?(s, "."), do: s, else: String.replace(s, "e", ".0e")
    {f, _} = Float.parse(s)
    f
  end

  defp duration_seconds(s) do
    ms =
      ~r/(\d+)(ms|[smhdwy])/
      |> Regex.scan(s)
      |> Enum.reduce(0, fn [_, n, unit], acc ->
        acc + String.to_integer(n) * unit_ms(unit)
      end)

    div(ms, 1000)
  end

  defp unit_ms("ms"), do: 1
  defp unit_ms("s"), do: 1_000
  defp unit_ms("m"), do: 60_000
  defp unit_ms("h"), do: 3_600_000
  defp unit_ms("d"), do: 86_400_000
  defp unit_ms("w"), do: 604_800_000
  defp unit_ms("y"), do: 31_536_000_000

  defp lex_ident(<<c, rest::binary>>, acc)
       when c in ?a..?z or c in ?A..?Z or c in ?0..?9 or c == ?_ or c == ?: do
    lex_ident(rest, [c | acc])
  end

  defp lex_ident(rest, acc), do: {acc |> Enum.reverse() |> List.to_string(), rest}

  defp describe_token({:ident, name}), do: "identifier #{inspect(name)}"
  defp describe_token({:kw, kw}), do: "keyword #{kw}"
  defp describe_token({:number, n}), do: "number #{n}"
  defp describe_token({:duration, s}), do: "duration (#{s}s)"
  defp describe_token({:string, s}), do: "string #{inspect(s)}"
  defp describe_token({:op, op}), do: "operator #{op_text(op)}"
  defp describe_token(:lparen), do: "("
  defp describe_token(:rparen), do: ")"
  defp describe_token(:lbrace), do: "{"
  defp describe_token(:rbrace), do: "}"
  defp describe_token(:lbracket), do: "["
  defp describe_token(:rbracket), do: "]"
  defp describe_token(:comma), do: ","
  defp describe_token(:at), do: "@"

  defp op_text(:eq), do: "=="
  defp op_text(:neq), do: "!="
  defp op_text(:gt), do: ">"
  defp op_text(:lt), do: "<"
  defp op_text(:gte), do: ">="
  defp op_text(:lte), do: "<="
  defp op_text(:re), do: "=~"
  defp op_text(:nre), do: "!~"
  defp op_text(:assign), do: "="
  defp op_text(:add), do: "+"
  defp op_text(:sub), do: "-"
  defp op_text(:mul), do: "*"
  defp op_text(:div), do: "/"
  defp op_text(:mod), do: "%"
  defp op_text(:pow), do: "^"

  # ===== Parser =====
  #
  # AST nodes:
  #   {:number, float}
  #   {:selector, %{metric, pattern, labels, offset}}
  #   {:range, selector_node, seconds}
  #   {:call, fname, [args]}
  #   {:agg, op, grouping, param, expr}   grouping :: {:by, [..]} | {:without, [..]} | nil
  #   {:binop, op, bool?, lhs, rhs}

  defp parse_expr(tokens), do: parse_or(tokens)

  defp parse_or(tokens) do
    with {:ok, left, rest} <- parse_and(tokens), do: parse_or_loop(left, rest)
  end

  defp parse_or_loop(left, [{:kw, :or} | rest]) do
    with {:ok, matching, rest} <- parse_matching(rest),
         {:ok, right, rest2} <- parse_and(rest) do
      parse_or_loop({:binop, :or, binop_opts(false, matching), left, right}, rest2)
    end
  end

  defp parse_or_loop(left, rest), do: {:ok, left, rest}

  defp parse_and(tokens) do
    with {:ok, left, rest} <- parse_cmp(tokens), do: parse_and_loop(left, rest)
  end

  defp parse_and_loop(left, [{:kw, op} | rest]) when op in [:and, :unless] do
    with {:ok, matching, rest} <- parse_matching(rest),
         {:ok, right, rest2} <- parse_cmp(rest) do
      parse_and_loop({:binop, op, binop_opts(false, matching), left, right}, rest2)
    end
  end

  defp parse_and_loop(left, rest), do: {:ok, left, rest}

  defp parse_cmp(tokens) do
    with {:ok, left, rest} <- parse_add(tokens), do: parse_cmp_loop(left, rest)
  end

  defp parse_cmp_loop(left, [{:op, op} | rest])
       when op in [:eq, :neq, :gt, :lt, :gte, :lte] do
    {bool?, rest} =
      case rest do
        [{:kw, :bool} | r] -> {true, r}
        _ -> {false, rest}
      end

    with {:ok, matching, rest} <- parse_matching(rest),
         {:ok, right, rest2} <- parse_add(rest) do
      parse_cmp_loop({:binop, op, binop_opts(bool?, matching), left, right}, rest2)
    end
  end

  defp parse_cmp_loop(left, rest), do: {:ok, left, rest}

  defp parse_add(tokens) do
    with {:ok, left, rest} <- parse_mul(tokens), do: parse_add_loop(left, rest)
  end

  defp parse_add_loop(left, [{:op, op} | rest]) when op in [:add, :sub] do
    with {:ok, matching, rest} <- parse_matching(rest),
         {:ok, right, rest2} <- parse_mul(rest) do
      parse_add_loop({:binop, op, binop_opts(false, matching), left, right}, rest2)
    end
  end

  defp parse_add_loop(left, rest), do: {:ok, left, rest}

  defp parse_mul(tokens) do
    with {:ok, left, rest} <- parse_unary(tokens), do: parse_mul_loop(left, rest)
  end

  defp parse_mul_loop(left, [{:op, op} | rest]) when op in [:mul, :div, :mod] do
    with {:ok, matching, rest} <- parse_matching(rest),
         {:ok, right, rest2} <- parse_unary(rest) do
      parse_mul_loop({:binop, op, binop_opts(false, matching), left, right}, rest2)
    end
  end

  defp parse_mul_loop(left, rest), do: {:ok, left, rest}

  # on(...)/ignoring(...) with optional group_left(...)/group_right(...)
  defp parse_matching([{:kw, kw} | rest]) when kw in [:on, :ignoring] do
    with {:ok, {_kw, labels}, rest2} <- parse_grouping(kw, rest) do
      case rest2 do
        [{:kw, gkw} | rest3] when gkw in [:group_left, :group_right] ->
          side = if gkw == :group_left, do: :left, else: :right

          case rest3 do
            [:lparen | _] ->
              with {:ok, {_gkw, extras}, rest4} <- parse_grouping(gkw, rest3) do
                {:ok, %{mode: kw, labels: labels, group: {side, extras}}, rest4}
              end

            _ ->
              {:ok, %{mode: kw, labels: labels, group: {side, []}}, rest3}
          end

        _ ->
          {:ok, %{mode: kw, labels: labels, group: nil}, rest2}
      end
    end
  end

  defp parse_matching([{:kw, gkw} | _]) when gkw in [:group_left, :group_right],
    do: {:error, "group_left/group_right must follow on(...) or ignoring(...)"}

  defp parse_matching(tokens), do: {:ok, nil, tokens}

  defp binop_opts(bool?, matching), do: %{bool: bool?, matching: matching}

  defp parse_unary([{:op, :sub} | rest]) do
    with {:ok, node, rest2} <- parse_unary(rest) do
      node =
        case node do
          {:number, n} -> {:number, -n}
          other -> {:binop, :mul, binop_opts(false, nil), {:number, -1.0}, other}
        end

      {:ok, node, rest2}
    end
  end

  defp parse_unary([{:op, :add} | rest]), do: parse_unary(rest)
  defp parse_unary(tokens), do: parse_pow(tokens)

  defp parse_pow(tokens) do
    with {:ok, left, rest} <- parse_atom(tokens) do
      case rest do
        [{:op, :pow} | rest2] ->
          with {:ok, matching, rest2} <- parse_matching(rest2),
               {:ok, right, rest3} <- parse_unary(rest2) do
            {:ok, {:binop, :pow, binop_opts(false, matching), left, right}, rest3}
          end

        _ ->
          {:ok, left, rest}
      end
    end
  end

  defp parse_atom([{:number, n} | rest]), do: {:ok, {:number, n * 1.0}, rest}

  defp parse_atom([:lparen | rest]) do
    with {:ok, node, rest2} <- parse_expr(rest) do
      case rest2 do
        [:rparen, :lbracket | _] ->
          {:error, "subqueries like (expr)[5m:] are not supported"}

        [:rparen | rest3] ->
          {:ok, node, rest3}

        [t | _] ->
          {:error, "expected ) but found #{describe_token(t)}"}

        [] ->
          {:error, "expected ) but query ended"}
      end
    end
  end

  defp parse_atom([:lbrace | _] = tokens), do: parse_selector(nil, tokens)

  defp parse_atom([{:ident, name} | rest]) do
    cond do
      name in @agg_ops and
          (match?([:lparen | _], rest) or
             match?([{:kw, kw} | _] when kw in [:by, :without], rest)) ->
        parse_agg(name, nil, rest)

      match?([:lparen | _], rest) ->
        fname = safe_atom(name)

        cond do
          fname in @functions ->
            parse_call(fname, rest)

          name in @known_unsupported ->
            {:error, "function #{name}() is not supported yet"}

          name in @metricsql_fns ->
            {:error,
             "#{name}() is a VictoriaMetrics MetricsQL extension and is not supported (standard PromQL only)"}

          true ->
            {:error, "unknown function: #{name}"}
        end

      true ->
        parse_selector(name, rest)
    end
  end

  defp parse_atom([{:kw, kw} | _]), do: {:error, "unexpected keyword: #{kw}"}
  defp parse_atom([{:string, s} | rest]), do: {:ok, {:string, s}, rest}
  defp parse_atom([t | _]), do: {:error, "unexpected #{describe_token(t)}"}
  defp parse_atom([]), do: {:error, "unexpected end of query"}

  defp safe_atom(name) do
    String.to_existing_atom(name)
  rescue
    ArgumentError -> nil
  end

  # Aggregation: op [by/without (labels)] ( [param ,] expr ) [by/without (labels)]
  defp parse_agg(name, grouping, [{:kw, kw} | rest]) when kw in [:by, :without] do
    if grouping do
      {:error, "duplicate grouping clause on #{name}()"}
    else
      with {:ok, g, rest2} <- parse_grouping(kw, rest) do
        parse_agg(name, g, rest2)
      end
    end
  end

  defp parse_agg(name, grouping, [:lparen | rest]) do
    op = String.to_existing_atom(name)

    with {:ok, param, expr, rest2} <- parse_agg_args(op, rest) do
      case {grouping, rest2} do
        {nil, [{:kw, kw} | rest3]} when kw in [:by, :without] ->
          with {:ok, g, rest4} <- parse_grouping(kw, rest3) do
            {:ok, {:agg, op, g, param, expr}, rest4}
          end

        _ ->
          {:ok, {:agg, op, grouping, param, expr}, rest2}
      end
    end
  end

  defp parse_agg(name, _grouping, _tokens),
    do: {:error, "expected ( after aggregation operator #{name}"}

  defp parse_agg_args(:count_values, [{:string, label}, :comma | rest]) do
    with {:ok, expr, rest2} <- parse_expr(rest),
         {:ok, rest3} <- expect_rparen(rest2, :count_values) do
      {:ok, {:string, label}, expr, rest3}
    end
  end

  defp parse_agg_args(:count_values, _tokens),
    do:
      {:error, ~s|count_values() requires a string label parameter: count_values("label", expr)|}

  defp parse_agg_args(op, tokens) when op in [:topk, :bottomk, :quantile] do
    with {:ok, param, rest} <- parse_expr(tokens) do
      case rest do
        [:comma | rest2] ->
          with {:ok, expr, rest3} <- parse_expr(rest2),
               {:ok, rest4} <- expect_rparen(rest3, op) do
            {:ok, param, expr, rest4}
          end

        _ ->
          {:error, "#{op}() requires a parameter: #{op}(k, expr)"}
      end
    end
  end

  defp parse_agg_args(op, tokens) do
    with {:ok, expr, rest} <- parse_expr(tokens),
         {:ok, rest2} <- expect_rparen(rest, op) do
      {:ok, nil, expr, rest2}
    end
  end

  defp expect_rparen([:rparen | rest], _op), do: {:ok, rest}

  defp expect_rparen([t | _], op),
    do: {:error, "expected ) to close #{op}(...), found #{describe_token(t)}"}

  defp expect_rparen([], op), do: {:error, "expected ) to close #{op}(...)"}

  defp parse_grouping(kw, [:lparen | rest]), do: parse_grouping_labels(kw, rest, [])

  defp parse_grouping(kw, _tokens), do: {:error, "expected ( after #{kw}"}

  defp parse_grouping_labels(kw, [:rparen | rest], acc),
    do: {:ok, {kw, Enum.reverse(acc)}, rest}

  defp parse_grouping_labels(kw, [{:ident, name} | rest], acc) do
    case rest do
      [:comma | rest2] -> parse_grouping_labels(kw, rest2, [name | acc])
      [:rparen | rest2] -> {:ok, {kw, Enum.reverse([name | acc])}, rest2}
      [t | _] -> {:error, "expected , or ) in #{kw} clause, found #{describe_token(t)}"}
      [] -> {:error, "unterminated #{kw} clause"}
    end
  end

  defp parse_grouping_labels(kw, [{:kw, name} | rest], acc),
    do: parse_grouping_labels(kw, [{:ident, Atom.to_string(name)} | rest], acc)

  defp parse_grouping_labels(kw, [t | _], _acc),
    do: {:error, "expected label name in #{kw} clause, found #{describe_token(t)}"}

  defp parse_grouping_labels(kw, [], _acc), do: {:error, "unterminated #{kw} clause"}

  defp parse_call(fname, [:lparen | rest]) do
    with {:ok, args, rest2} <- parse_args(rest, []) do
      {:ok, {:call, fname, args}, rest2}
    end
  end

  defp parse_args([:rparen | rest], acc), do: {:ok, Enum.reverse(acc), rest}

  defp parse_args(tokens, acc) do
    with {:ok, node, rest} <- parse_expr(tokens) do
      case rest do
        [:comma | rest2] -> parse_args(rest2, [node | acc])
        [:rparen | rest2] -> {:ok, Enum.reverse([node | acc]), rest2}
        [t | _] -> {:error, "expected , or ) in function arguments, found #{describe_token(t)}"}
        [] -> {:error, "unterminated function call"}
      end
    end
  end

  defp parse_selector(name, [:lbrace | rest]) do
    with {:ok, matchers, rest2} <- parse_matchers(rest, []),
         {:ok, sel} <- build_selector(name, matchers) do
      parse_selector_postfix({:selector, sel}, rest2)
    end
  end

  defp parse_selector(name, tokens) when is_binary(name) do
    with {:ok, sel} <- build_selector(name, []) do
      parse_selector_postfix({:selector, sel}, tokens)
    end
  end

  # Matchers accumulate as a LIST — Prometheus ANDs duplicate labels
  # ({job=~"a.*", job!~"a-dev"} is legal), so a map would silently drop
  # matchers.
  defp parse_matchers([:rbrace | rest], acc), do: {:ok, Enum.reverse(acc), rest}

  defp parse_matchers(tokens, acc) do
    with {:ok, {k, v}, rest} <- parse_matcher(tokens) do
      acc = [{k, v} | acc]

      case rest do
        [:comma, :rbrace | rest2] -> {:ok, Enum.reverse(acc), rest2}
        [:comma | rest2] -> parse_matchers(rest2, acc)
        [:rbrace | rest2] -> {:ok, Enum.reverse(acc), rest2}
        [t | _] -> {:error, "expected , or } in label matchers, found #{describe_token(t)}"}
        [] -> {:error, "unterminated label matcher block"}
      end
    end
  end

  defp parse_matcher([{:kw, k} | rest]),
    do: parse_matcher([{:ident, Atom.to_string(k)} | rest])

  defp parse_matcher([{:ident, k}, {:op, op}, {:string, v} | rest])
       when op in [:assign, :re, :neq, :nre] do
    value =
      case op do
        :assign -> v
        :re -> {:regex, v}
        :neq -> {:not_equal, v}
        :nre -> {:not_regex, v}
      end

    {:ok, {k, value}, rest}
  end

  defp parse_matcher(_tokens) do
    {:error,
     ~s(invalid label matcher — expected label="value", label!="v", label=~"re", or label!~"re")}
  end

  defp build_selector(name, matchers) do
    {name_matchers, labels} = Enum.split_with(matchers, fn {k, _v} -> k == "__name__" end)

    case {name, name_matchers} do
      {nil, []} when labels == [] ->
        {:error, "selector must specify a metric name or at least one label matcher"}

      {nil, []} ->
        {:ok, %{metric: nil, pattern: ".+", labels: labels, offset: 0}}

      {name, []} ->
        {:ok, %{metric: name, pattern: nil, labels: labels, offset: 0}}

      {nil, [{_, exact}]} when is_binary(exact) ->
        {:ok, %{metric: exact, pattern: nil, labels: labels, offset: 0}}

      {nil, [{_, {:regex, p}}]} ->
        {:ok, %{metric: nil, pattern: p, labels: labels, offset: 0}}

      {name, [_ | _]} when is_binary(name) ->
        {:error, "metric name specified twice (as name and __name__ matcher)"}

      {nil, [_ | _]} ->
        {:error, "unsupported __name__ matcher (negative or multiple __name__ matchers)"}
    end
  end

  defp parse_selector_postfix(node, [:lbracket | rest]) do
    case rest do
      [{:duration, secs}, :rbracket | rest2] ->
        parse_offset_postfix({:range, node, secs}, rest2)

      [{:number, n}, :rbracket | rest2] ->
        parse_offset_postfix({:range, node, trunc(n)}, rest2)

      [{:duration, _}, {:ident, ":" <> _} | _] ->
        {:error, "subqueries like metric[5m:1m] are not supported"}

      _ ->
        {:error, "expected a duration like [5m]"}
    end
  end

  defp parse_selector_postfix(node, tokens), do: parse_offset_postfix(node, tokens)

  defp parse_offset_postfix(node, [{:kw, :offset} | rest]) do
    case rest do
      [{:duration, secs} | rest2] -> {:ok, apply_offset(node, secs), rest2}
      [{:number, n} | rest2] -> {:ok, apply_offset(node, trunc(n)), rest2}
      [{:op, :sub}, {:duration, secs} | rest2] -> {:ok, apply_offset(node, -secs), rest2}
      _ -> {:error, "expected a duration after offset"}
    end
  end

  defp parse_offset_postfix(_node, [:at | _]),
    do: {:error, "the @ modifier is not supported"}

  defp parse_offset_postfix(node, tokens), do: {:ok, node, tokens}

  defp apply_offset({:range, sel_node, d}, secs), do: {:range, apply_offset(sel_node, secs), d}
  defp apply_offset({:selector, sel}, secs), do: {:selector, %{sel | offset: secs}}

  # ===== Evaluator =====
  #
  # eval/2 returns {:ok, {:vector, [%{labels: map, data: [{ts, val}]}]}}
  #              | {:ok, {:scalar, number}}
  #              | {:error, reason}
  # Values are numbers, or :inf | :neg_inf | :nan after arithmetic.

  defp eval({:number, n}, _ctx), do: {:ok, {:scalar, n}}

  defp eval({:string, _s}, _ctx),
    do: {:error, "string literals are only valid as function arguments"}

  # Instant selector: at each grid point, the most recent sample within the
  # lookback window (keeps __name__, like Prometheus).
  defp eval({:selector, sel}, ctx) do
    with {:ok, series} <- eval_windowed(sel, ctx.lookback, true, ctx, &window_last/3) do
      {:ok, {:vector, series}}
    end
  end

  defp eval({:range, _, _}, _ctx) do
    {:error,
     "range vector selector must be wrapped in a function like rate(...) or avg_over_time(...)"}
  end

  defp eval({:call, f, args}, ctx), do: eval_call(f, args, ctx)
  defp eval({:agg, op, grouping, param, expr}, ctx), do: eval_agg(op, grouping, param, expr, ctx)
  defp eval({:binop, op, opts, l, r}, ctx), do: eval_binop(op, opts, l, r, ctx)

  defp eval_vector(node, ctx) do
    case eval(node, ctx) do
      {:ok, {:vector, series}} -> {:ok, series}
      {:ok, {:scalar, _}} -> {:error, "expected an instant vector, got a scalar"}
      {:error, _} = err -> err
    end
  end

  defp eval_scalar(node, ctx) do
    case eval(node, ctx) do
      {:ok, {:scalar, n}} -> {:ok, n}
      {:ok, {:vector, _}} -> {:error, "expected a scalar (number), got an instant vector"}
      {:error, _} = err -> err
    end
  end

  # --- function calls ---

  # Range-vector functions evaluate their window fn over the raw samples in
  # (T - window, T] at each grid point T. Counter functions additionally see
  # the sample just before the window (carry-in) for accurate increase math.
  defp eval_call(f, [arg], ctx) when f in @rollup_fns do
    case arg do
      {:range, {:selector, sel}, window} when window > 0 ->
        keep_name = f in @name_keeping_rollups

        with {:ok, series} <-
               eval_windowed(sel, window, keep_name, ctx, rollup_window_fun(f, window)) do
          {:ok, {:vector, series}}
        end

      {:range, {:selector, _sel}, _zero} ->
        {:error, "#{f}() requires a non-zero range window"}

      _ ->
        {:error, "#{f}() expects a range vector argument like metric[5m]"}
    end
  end

  defp eval_call(f, [arg], ctx) when f in @transform_fns do
    with {:ok, series} <- eval_vector(arg, ctx) do
      series = series |> map_values(&transform_value(f, &1)) |> apply_name_policy(f)
      {:ok, {:vector, series}}
    end
  end

  defp eval_call(:round, [arg, nearest], ctx) do
    with {:ok, series} <- eval_vector(arg, ctx),
         {:ok, n} <- eval_scalar(nearest, ctx) do
      series =
        series
        |> map_values(fn v ->
          if is_number(v) and n != 0, do: Float.round(v / n) * n, else: v
        end)

      {:ok, {:vector, series}}
    end
  end

  defp eval_call(:clamp, [arg, mn, mx], ctx) do
    with {:ok, series} <- eval_vector(arg, ctx),
         {:ok, mn} <- eval_scalar(mn, ctx),
         {:ok, mx} <- eval_scalar(mx, ctx) do
      series =
        series
        |> map_values(fn v -> if is_number(v), do: v |> max(mn) |> min(mx), else: v end)

      {:ok, {:vector, series}}
    end
  end

  defp eval_call(:clamp_min, [arg, mn], ctx) do
    with {:ok, series} <- eval_vector(arg, ctx),
         {:ok, mn} <- eval_scalar(mn, ctx) do
      series =
        series
        |> map_values(fn v -> if is_number(v), do: max(v, mn), else: v end)

      {:ok, {:vector, series}}
    end
  end

  defp eval_call(:clamp_max, [arg, mx], ctx) do
    with {:ok, series} <- eval_vector(arg, ctx),
         {:ok, mx} <- eval_scalar(mx, ctx) do
      series =
        series
        |> map_values(fn v -> if is_number(v), do: min(v, mx), else: v end)

      {:ok, {:vector, series}}
    end
  end

  defp eval_call(:pi, [], _ctx), do: {:ok, {:scalar, :math.pi()}}

  defp eval_call(:quantile_over_time, [phi_node, {:range, {:selector, sel}, window}], ctx)
       when window > 0 do
    with {:ok, phi} <- eval_scalar(phi_node, ctx) do
      fun =
        stat_fun(fn vals ->
          cond do
            phi < 0 -> :neg_inf
            phi > 1 -> :inf
            true -> quantile(Enum.sort(vals), phi)
          end
        end)

      with {:ok, series} <- eval_windowed(sel, window, true, ctx, fun) do
        {:ok, {:vector, series}}
      end
    end
  end

  defp eval_call(:quantile_over_time, [_phi, _arg], _ctx),
    do: {:error, "quantile_over_time() expects (scalar, metric[5m])"}

  defp eval_call(:predict_linear, [{:range, {:selector, sel}, window}, t_node], ctx)
       when window > 0 do
    with {:ok, horizon} <- eval_scalar(t_node, ctx) do
      fun = fn
        slice, _prev, t when length(slice) >= 2 ->
          case linear_regression(slice, t) do
            {:nan, _} -> :nan
            {slope, intercept} -> intercept + slope * horizon
          end

        [{_ts, v}], _prev, _t ->
          v

        [], _prev, _t ->
          :skip
      end

      with {:ok, series} <- eval_windowed(sel, window, true, ctx, fun) do
        {:ok, {:vector, series}}
      end
    end
  end

  defp eval_call(:predict_linear, [_arg, _t], _ctx),
    do: {:error, "predict_linear() expects (metric[5m], scalar)"}

  @label_name_re ~r/^[a-zA-Z_][a-zA-Z0-9_]*$/

  defp eval_call(
         :label_replace,
         [arg, {:string, dst}, {:string, repl}, {:string, src}, {:string, regex}],
         ctx
       ) do
    with :ok <- validate_label_name(dst),
         {:ok, re} <- compile_anchored(regex),
         {:ok, series} <- eval_vector(arg, ctx) do
      series =
        Enum.map(series, fn %{labels: labels} = s ->
          src_val = Map.get(labels, src, "")

          case Regex.run(re, src_val) do
            nil ->
              s

            captures ->
              case expand_template(repl, captures) do
                "" -> %{s | labels: Map.delete(labels, dst)}
                new_val -> %{s | labels: Map.put(labels, dst, new_val)}
              end
          end
        end)

      {:ok, {:vector, series}}
    end
  end

  defp eval_call(:label_replace, args, _ctx) when length(args) == 5,
    do: {:error, "label_replace() arguments 2-5 must be string literals"}

  defp eval_call(:label_join, [arg, {:string, dst}, {:string, sep} | srcs], ctx)
       when srcs != [] do
    with :ok <- validate_label_name(dst),
         {:ok, src_names} <- string_args(srcs, "label_join() source labels"),
         {:ok, series} <- eval_vector(arg, ctx) do
      series =
        Enum.map(series, fn %{labels: labels} = s ->
          case Enum.map_join(src_names, sep, &Map.get(labels, &1, "")) do
            "" -> %{s | labels: Map.delete(labels, dst)}
            joined -> %{s | labels: Map.put(labels, dst, joined)}
          end
        end)

      {:ok, {:vector, series}}
    end
  end

  defp eval_call(:label_join, args, _ctx) when length(args) >= 3,
    do: {:error, "label_join() expects (vector, \"dst\", \"separator\", \"src\", ...)"}

  defp eval_call(:histogram_quantile, [phi_node, arg], ctx) do
    with {:ok, phi} <- eval_scalar(phi_node, ctx),
         {:ok, series} <- eval_vector(arg, ctx) do
      {:ok, {:vector, histogram_quantile_series(phi, series)}}
    end
  end

  defp eval_call(f, args, _ctx),
    do: {:error, "#{f}() called with #{length(args)} argument(s) — wrong arity"}

  # Classic-histogram quantile over le-labeled cumulative buckets, following
  # Prometheus' histogramQuantile: monotonic fixup, linear interpolation
  # within the chosen bucket, largest finite le when the +Inf bucket wins.
  defp histogram_quantile_series(phi, series) do
    series
    |> Enum.filter(fn %{labels: l} -> Map.has_key?(l, "le") end)
    |> Enum.group_by(fn %{labels: l} -> l |> Map.delete("le") |> Map.delete("__name__") end)
    |> Enum.flat_map(fn {group_labels, buckets} ->
      parsed =
        buckets
        |> Enum.flat_map(fn %{labels: l, data: data} ->
          case parse_le(Map.fetch!(l, "le")) do
            nil -> []
            le -> [{le, Map.new(data)}]
          end
        end)
        |> Enum.sort_by(fn {le, _} -> le_rank(le) end)

      ts_all =
        parsed
        |> Enum.flat_map(fn {_le, m} -> Map.keys(m) end)
        |> Enum.uniq()
        |> Enum.sort()

      data =
        Enum.flat_map(ts_all, fn ts ->
          pairs =
            Enum.flat_map(parsed, fn {le, m} ->
              case Map.fetch(m, ts) do
                {:ok, v} when is_number(v) -> [{le, v}]
                _ -> []
              end
            end)

          case bucket_quantile(phi, pairs) do
            :skip -> []
            v -> [{ts, v}]
          end
        end)

      if data == [], do: [], else: [%{labels: group_labels, data: data}]
    end)
  end

  defp parse_le("+Inf"), do: :inf
  defp parse_le("Inf"), do: :inf

  defp parse_le(str) do
    case Float.parse(str) do
      {f, ""} -> f
      _ -> nil
    end
  end

  defp le_rank(:inf), do: {1, 0.0}
  defp le_rank(le), do: {0, le}

  defp bucket_quantile(_phi, pairs) when length(pairs) < 2, do: :skip

  defp bucket_quantile(phi, pairs) do
    # monotonic fixup: cumulative counts may glitch downward across buckets
    {fixed, _} =
      Enum.map_reduce(pairs, 0.0, fn {le, v}, running_max ->
        m = max(v, running_max)
        {{le, m}, m}
      end)

    {last_le, total} = List.last(fixed)

    cond do
      last_le != :inf -> :skip
      # VM omits the point entirely when the histogram has no observations
      total == 0 -> :skip
      phi < 0 -> :neg_inf
      phi > 1 -> :inf
      true -> interpolate_quantile(phi * total, fixed)
    end
  end

  defp interpolate_quantile(rank, fixed) do
    finite = Enum.reject(fixed, fn {le, _} -> le == :inf end)

    case Enum.find_index(fixed, fn {_le, cum} -> cum >= rank end) do
      nil ->
        :nan

      idx ->
        {le, cum} = Enum.at(fixed, idx)

        cond do
          le == :inf ->
            case List.last(finite) do
              nil -> :nan
              {max_le, _} -> max_le
            end

          idx == 0 ->
            if le <= 0, do: le, else: le * min(rank / cum, 1.0)

          true ->
            {prev_le, prev_cum} = Enum.at(fixed, idx - 1)
            count = cum - prev_cum

            if count <= 0 do
              le
            else
              prev_le + (le - prev_le) * ((rank - prev_cum) / count)
            end
        end
    end
  end

  defp validate_label_name(name) do
    if Regex.match?(@label_name_re, name) or name == "__name__" do
      :ok
    else
      {:error, "invalid destination label name: #{inspect(name)}"}
    end
  end

  defp string_args(nodes, what) do
    Enum.reduce_while(nodes, {:ok, []}, fn
      {:string, s}, {:ok, acc} -> {:cont, {:ok, acc ++ [s]}}
      _other, _acc -> {:halt, {:error, "#{what} must be string literals"}}
    end)
  end

  # $1..$9 capture references, $$ escapes a literal dollar
  defp expand_template(template, captures) do
    Regex.replace(~r/\$(\$|\d)/, template, fn _whole, ref ->
      case ref do
        "$" -> "$"
        d -> Enum.at(captures, String.to_integer(d), "")
      end
    end)
  end

  defp apply_name_policy(series, f) when f in @name_keeping_transforms, do: series
  defp apply_name_policy(series, _f), do: drop_names(series)

  defp transform_value(f, v) when is_number(v) do
    case f do
      :abs -> abs(v)
      :ceil -> Float.ceil(v * 1.0)
      :floor -> Float.floor(v * 1.0)
      :round -> Float.round(v * 1.0)
      :sqrt -> if v < 0, do: :nan, else: :math.sqrt(v)
      :exp -> :math.exp(v)
      :ln -> if v <= 0, do: log_special(v), else: :math.log(v)
      :log2 -> if v <= 0, do: log_special(v), else: :math.log2(v)
      :log10 -> if v <= 0, do: log_special(v), else: :math.log10(v)
      :sgn -> sgn(v)
      :deg -> v * 180.0 / :math.pi()
      :rad -> v * :math.pi() / 180.0
      other -> safe_math(other, v)
    end
  end

  defp transform_value(_f, v), do: v

  defp sgn(v) when v > 0, do: 1.0
  defp sgn(v) when v < 0, do: -1.0
  defp sgn(_v), do: 0.0

  # Trig via :math; out-of-domain arguments become NaN instead of raising
  defp safe_math(f, v) do
    apply(:math, f, [v * 1.0])
  rescue
    ArithmeticError -> :nan
  end

  defp log_special(v) when v == 0, do: :neg_inf
  defp log_special(_v), do: :nan

  # --- aggregation ---

  defp eval_agg(op, grouping, param, expr, ctx) do
    with {:ok, series} <- eval_vector(expr, ctx),
         {:ok, param_val} <- eval_agg_param(param, ctx) do
      do_aggregate(op, grouping, param_val, series)
    end
  end

  defp eval_agg_param(nil, _ctx), do: {:ok, nil}
  defp eval_agg_param({:string, s}, _ctx), do: {:ok, {:string, s}}
  defp eval_agg_param(node, ctx), do: eval_scalar(node, ctx)

  defp do_aggregate(:count_values, grouping, {:string, label}, series) do
    result =
      series
      |> Enum.group_by(&group_labels(&1.labels, grouping))
      |> Enum.flat_map(fn {key, group_series} ->
        group_series
        |> Enum.flat_map(& &1.data)
        |> Enum.group_by(fn {_ts, v} -> count_values_label(v) end)
        |> Enum.map(fn {value_str, points} ->
          data =
            points
            |> Enum.group_by(&elem(&1, 0))
            |> Enum.map(fn {ts, pts} -> {ts, length(pts) * 1.0} end)
            |> Enum.sort_by(&elem(&1, 0))

          %{labels: Map.put(key, label, value_str), data: data}
        end)
      end)

    {:ok, {:vector, result}}
  end

  defp do_aggregate(op, grouping, param, series) when op in [:topk, :bottomk] do
    k = trunc(param || 1)

    result =
      series
      |> Enum.group_by(&group_labels(&1.labels, grouping))
      |> Enum.flat_map(fn {_key, group_series} -> select_k(op, k, group_series) end)

    {:ok, {:vector, result}}
  end

  defp do_aggregate(op, grouping, param, series) do
    result =
      series
      |> Enum.group_by(&group_labels(&1.labels, grouping))
      |> Enum.map(fn {key, group_series} ->
        data =
          group_series
          |> Enum.flat_map(& &1.data)
          |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
          |> Enum.map(fn {ts, vals} -> {ts, agg_values(op, vals, param)} end)
          |> Enum.sort_by(&elem(&1, 0))

        %{labels: key, data: data}
      end)
      |> Enum.reject(&(&1.data == []))

    {:ok, {:vector, result}}
  end

  # VM/Prometheus format count_values label values compactly: 42, not 42.0
  defp count_values_label(v) when is_float(v) do
    t = trunc(v)
    if t * 1.0 == v, do: Integer.to_string(t), else: Float.to_string(v)
  end

  defp count_values_label(v), do: format_value(v)

  defp group_labels(labels, {:by, keys}), do: Map.take(labels, keys)

  defp group_labels(labels, {:without, keys}),
    do: labels |> Map.drop(keys) |> Map.delete("__name__")

  defp group_labels(_labels, nil), do: %{}

  # topk/bottomk keep original series labels; each timestamp ranks independently
  defp select_k(op, k, group_series) do
    indexed = Enum.with_index(group_series)

    kept_by_series =
      indexed
      |> Enum.flat_map(fn {%{data: data}, idx} ->
        Enum.map(data, fn {ts, v} -> {ts, v, idx} end)
      end)
      |> Enum.group_by(fn {ts, _v, _idx} -> ts end)
      |> Enum.flat_map(fn {_ts, points} ->
        points
        |> Enum.filter(fn {_ts, v, _idx} -> is_number(v) end)
        |> Enum.sort_by(fn {_ts, v, _idx} -> v end, if(op == :topk, do: :desc, else: :asc))
        |> Enum.take(k)
      end)
      |> Enum.group_by(fn {_ts, _v, idx} -> idx end)

    indexed
    |> Enum.flat_map(fn {%{labels: labels}, idx} ->
      case Map.get(kept_by_series, idx, []) do
        [] ->
          []

        points ->
          data = points |> Enum.map(fn {ts, v, _} -> {ts, v} end) |> Enum.sort_by(&elem(&1, 0))
          [%{labels: labels, data: data}]
      end
    end)
  end

  defp agg_values(op, vals, param) do
    cond do
      Enum.any?(vals, &(&1 == :nan)) -> :nan
      Enum.all?(vals, &is_number/1) -> agg_numbers(op, vals, param)
      true -> agg_with_inf(op, vals)
    end
  end

  defp agg_numbers(:sum, vals, _), do: Enum.sum(vals) * 1.0
  defp agg_numbers(:avg, vals, _), do: Enum.sum(vals) / length(vals)
  defp agg_numbers(:min, vals, _), do: Enum.min(vals) * 1.0
  defp agg_numbers(:max, vals, _), do: Enum.max(vals) * 1.0
  defp agg_numbers(:count, vals, _), do: length(vals) * 1.0
  defp agg_numbers(:group, _vals, _), do: 1.0

  defp agg_numbers(:stddev, vals, _), do: :math.sqrt(variance(vals))
  defp agg_numbers(:stdvar, vals, _), do: variance(vals)

  defp agg_numbers(:quantile, vals, q) when is_number(q) do
    cond do
      q < 0 -> :neg_inf
      q > 1 -> :inf
      true -> quantile(Enum.sort(vals), q)
    end
  end

  defp variance(vals) do
    n = length(vals)
    mean = Enum.sum(vals) / n
    Enum.reduce(vals, 0.0, fn v, acc -> acc + (v - mean) * (v - mean) end) / n
  end

  defp quantile([v], _q), do: v * 1.0

  defp quantile(sorted, q) do
    n = length(sorted)
    rank = q * (n - 1)
    lo = trunc(rank)
    hi = min(lo + 1, n - 1)
    frac = rank - lo
    Enum.at(sorted, lo) * (1 - frac) + Enum.at(sorted, hi) * frac
  end

  # Coarse infinity handling for aggregation inputs
  defp agg_with_inf(op, vals) when op in [:min, :max] do
    sorted = Enum.sort_by(vals, &inf_rank/1)
    if op == :min, do: hd(sorted), else: List.last(sorted)
  end

  defp agg_with_inf(:count, vals), do: length(vals) * 1.0
  defp agg_with_inf(:group, _vals), do: 1.0

  defp agg_with_inf(op, vals) when op in [:sum, :avg] do
    has_pos = :inf in vals
    has_neg = :neg_inf in vals

    cond do
      has_pos and has_neg -> :nan
      has_pos -> :inf
      true -> :neg_inf
    end
  end

  defp agg_with_inf(_op, _vals), do: :nan

  defp inf_rank(:neg_inf), do: {-1, 0}
  defp inf_rank(:inf), do: {1, 0}
  defp inf_rank(v), do: {0, v}

  # --- binary operators ---

  @comparison_ops [:eq, :neq, :gt, :lt, :gte, :lte]

  defp eval_binop(op, opts, l, r, ctx) do
    with {:ok, lv} <- eval(l, ctx),
         {:ok, rv} <- eval(r, ctx) do
      apply_binop(op, opts, lv, rv)
    end
  end

  # set operators
  defp apply_binop(op, opts, {:vector, ls}, {:vector, rs}) when op in [:and, :or, :unless] do
    case opts.matching do
      %{group: {_side, _extras}} ->
        {:error, "group_left/group_right are not allowed with #{op}"}

      matching ->
        {:ok, {:vector, set_op(op, ls, rs, matching)}}
    end
  end

  defp apply_binop(op, _opts, _l, _r) when op in [:and, :or, :unless] do
    {:error, "#{op} requires instant vectors on both sides"}
  end

  # scalar ∘ scalar
  defp apply_binop(op, opts, {:scalar, a}, {:scalar, b}) do
    cond do
      op not in @comparison_ops -> {:ok, {:scalar, arith(op, a, b)}}
      opts.bool -> {:ok, {:scalar, if(cmp(op, a, b), do: 1.0, else: 0.0)}}
      true -> {:error, "comparisons between scalars must use the bool modifier (e.g. 1 > bool 2)"}
    end
  end

  # vector ∘ scalar / scalar ∘ vector — matching modifiers don't apply
  defp apply_binop(_op, %{matching: matching}, l, r)
       when matching != nil and (elem(l, 0) == :scalar or elem(r, 0) == :scalar) do
    {:error, "vector matching (on/ignoring) is only allowed between two instant vectors"}
  end

  defp apply_binop(op, opts, {:vector, series}, {:scalar, n}),
    do: vector_scalar(op, opts.bool, series, n, :scalar_right)

  defp apply_binop(op, opts, {:scalar, n}, {:vector, series}),
    do: vector_scalar(op, opts.bool, series, n, :scalar_left)

  # vector ∘ vector
  defp apply_binop(op, opts, {:vector, ls}, {:vector, rs}) do
    if op in @comparison_ops do
      vector_vector_cmp(op, opts, ls, rs)
    else
      vector_vector_arith(op, opts, ls, rs)
    end
  end

  defp vector_scalar(op, bool?, series, n, orient) do
    apply_op = fn v ->
      case orient do
        :scalar_right -> {op, v, n}
        :scalar_left -> {op, n, v}
      end
    end

    result =
      cond do
        op not in @comparison_ops ->
          series
          |> map_values(fn v ->
            {o, a, b} = apply_op.(v)
            arith(o, a, b)
          end)
          |> drop_names()

        bool? ->
          series
          |> map_values(fn v ->
            {o, a, b} = apply_op.(v)
            if cmp(o, a, b), do: 1.0, else: 0.0
          end)
          |> drop_names()

        true ->
          # filtering comparison: keep samples where the predicate holds
          series
          |> Enum.map(fn %{data: data} = s ->
            %{
              s
              | data:
                  Enum.filter(data, fn {_ts, v} ->
                    {o, a, b} = apply_op.(v)
                    cmp(o, a, b)
                  end)
            }
          end)
          |> Enum.reject(&(&1.data == []))
      end

    {:ok, {:vector, result}}
  end

  # --- vector-vector matching ---

  defp match_sig(labels, nil), do: Map.delete(labels, "__name__")
  defp match_sig(labels, %{mode: :on, labels: ls}), do: Map.take(labels, ls)

  defp match_sig(labels, %{mode: :ignoring, labels: ls}),
    do: labels |> Map.drop(ls) |> Map.delete("__name__")

  # For group_left the left side is the "many" side; group_right mirrors.
  defp orient_sides(ls, rs, %{group: {:right, _}}), do: {rs, ls, :swapped}
  defp orient_sides(ls, rs, _matching), do: {ls, rs, :normal}

  defp vector_vector_arith(op, opts, ls, rs) do
    matching = opts.matching

    case matching do
      %{group: {_side, extras}} ->
        {many, one, orientation} = orient_sides(ls, rs, matching)

        with {:ok, one_map} <- sig_map(one, matching, "the \"one\" side") do
          series =
            Enum.flat_map(many, fn %{labels: labels, data: data} ->
              sig = match_sig(labels, matching)

              case Map.fetch(one_map, sig) do
                {:ok, {one_labels, one_data}} ->
                  joined = join_arith(op, data, one_data, orientation)

                  if joined == [] do
                    []
                  else
                    out_labels =
                      labels
                      |> Map.delete("__name__")
                      |> copy_extra_labels(one_labels, extras)

                    [%{labels: out_labels, data: joined}]
                  end

                :error ->
                  []
              end
            end)

          {:ok, {:vector, series}}
        end

      _ ->
        with {:ok, rmap} <- sig_map(rs, matching, "the right side"),
             {:ok, _} <- sig_map(ls, matching, "the left side") do
          series =
            Enum.flat_map(ls, fn %{labels: labels, data: data} ->
              sig = match_sig(labels, matching)

              case Map.fetch(rmap, sig) do
                {:ok, {_rlabels, rdata}} ->
                  joined = join_arith(op, data, rdata, :normal)
                  if joined == [], do: [], else: [%{labels: sig, data: joined}]

                :error ->
                  []
              end
            end)

          {:ok, {:vector, series}}
        end
    end
  end

  defp join_arith(op, many_data, one_map, orientation) do
    Enum.flat_map(many_data, fn {ts, v} ->
      case Map.fetch(one_map, ts) do
        {:ok, ov} ->
          value =
            case orientation do
              :normal -> arith(op, v, ov)
              :swapped -> arith(op, ov, v)
            end

          [{ts, value}]

        :error ->
          []
      end
    end)
  end

  defp copy_extra_labels(labels, from_labels, extras) do
    Enum.reduce(extras, labels, fn k, acc ->
      case Map.fetch(from_labels, k) do
        {:ok, v} -> Map.put(acc, k, v)
        :error -> Map.delete(acc, k)
      end
    end)
  end

  defp vector_vector_cmp(op, opts, ls, rs) do
    matching = opts.matching

    {many, one, orientation} = orient_sides(ls, rs, matching)

    with {:ok, one_map} <- sig_map(one, matching, "one side"),
         :ok <- check_unique_when_one_to_one(matching, many) do
      series =
        Enum.flat_map(many, fn %{labels: labels, data: data} = s ->
          sig = match_sig(labels, matching)

          case Map.fetch(one_map, sig) do
            {:ok, {_olabels, odata}} ->
              matched =
                Enum.flat_map(data, fn {ts, v} ->
                  case Map.fetch(odata, ts) do
                    {:ok, ov} ->
                      {a, b} = if orientation == :normal, do: {v, ov}, else: {ov, v}

                      cond do
                        opts.bool -> [{ts, if(cmp(op, a, b), do: 1.0, else: 0.0)}]
                        cmp(op, a, b) -> [{ts, v}]
                        true -> []
                      end

                    :error ->
                      []
                  end
                end)

              cond do
                matched == [] ->
                  []

                opts.bool ->
                  [%{labels: Map.delete(labels, "__name__"), data: matched}]

                # with a matching modifier VM drops __name__ even on
                # filtering comparisons (verified via vm_diff)
                matching != nil ->
                  [%{s | labels: Map.delete(labels, "__name__"), data: matched}]

                true ->
                  [%{s | data: matched}]
              end

            :error ->
              []
          end
        end)

      {:ok, {:vector, series}}
    end
  end

  defp check_unique_when_one_to_one(%{group: {_side, _}}, _series), do: :ok

  defp check_unique_when_one_to_one(matching, series) do
    case sig_map(series, matching, "one side") do
      {:ok, _} -> :ok
      {:error, _} = err -> err
    end
  end

  # Map sig -> {labels, %{ts => value}}; errors when two series share a sig.
  defp sig_map(series, matching, side_desc) do
    Enum.reduce_while(series, {:ok, %{}}, fn %{labels: l, data: d}, {:ok, acc} ->
      sig = match_sig(l, matching)

      if Map.has_key?(acc, sig) do
        {:halt,
         {:error,
          "many-to-many vector matching: multiple series on #{side_desc} share the match group #{inspect(sig)} — use group_left/group_right or aggregate first"}}
      else
        {:cont, {:ok, Map.put(acc, sig, {l, Map.new(d)})}}
      end
    end)
  end

  defp set_op(:and, ls, rs, matching) do
    rsigs =
      Enum.reduce(rs, %{}, fn %{labels: l, data: d}, acc ->
        sig = match_sig(l, matching)

        Map.update(
          acc,
          sig,
          MapSet.new(d, &elem(&1, 0)),
          &MapSet.union(&1, MapSet.new(d, fn p -> elem(p, 0) end))
        )
      end)

    ls
    |> Enum.flat_map(fn %{labels: l, data: data} = s ->
      case Map.fetch(rsigs, match_sig(l, matching)) do
        {:ok, ts_set} ->
          kept = Enum.filter(data, fn {ts, _} -> MapSet.member?(ts_set, ts) end)
          if kept == [], do: [], else: [%{s | data: kept}]

        :error ->
          []
      end
    end)
  end

  defp set_op(:unless, ls, rs, matching) do
    rsigs =
      Enum.reduce(rs, %{}, fn %{labels: l, data: d}, acc ->
        sig = match_sig(l, matching)

        Map.update(
          acc,
          sig,
          MapSet.new(d, &elem(&1, 0)),
          &MapSet.union(&1, MapSet.new(d, fn p -> elem(p, 0) end))
        )
      end)

    ls
    |> Enum.flat_map(fn %{labels: l, data: data} = s ->
      case Map.fetch(rsigs, match_sig(l, matching)) do
        {:ok, ts_set} ->
          kept = Enum.reject(data, fn {ts, _} -> MapSet.member?(ts_set, ts) end)
          if kept == [], do: [], else: [%{s | data: kept}]

        :error ->
          [s]
      end
    end)
  end

  defp set_op(:or, ls, rs, matching) do
    lmap =
      Enum.reduce(ls, %{}, fn %{labels: l, data: d}, acc ->
        sig = match_sig(l, matching)

        Map.update(
          acc,
          sig,
          MapSet.new(d, &elem(&1, 0)),
          &MapSet.union(&1, MapSet.new(d, fn p -> elem(p, 0) end))
        )
      end)

    extra =
      rs
      |> Enum.flat_map(fn %{labels: l, data: data} = s ->
        case Map.fetch(lmap, match_sig(l, matching)) do
          {:ok, lts} ->
            missing = Enum.reject(data, fn {ts, _} -> MapSet.member?(lts, ts) end)
            if missing == [], do: [], else: [%{s | data: missing}]

          :error ->
            [s]
        end
      end)

    ls ++ extra
  end

  # --- arithmetic on values (numbers plus :inf/:neg_inf/:nan) ---

  defp arith(_op, :nan, _b), do: :nan
  defp arith(_op, _a, :nan), do: :nan

  defp arith(op, a, b) when is_number(a) and is_number(b) do
    case op do
      :add -> a + b
      :sub -> a - b
      :mul -> a * b
      :div -> safe_div(a, b)
      :mod -> if b == 0, do: :nan, else: :math.fmod(a * 1.0, b * 1.0)
      :pow -> safe_pow(a, b)
    end
  end

  # coarse infinity propagation
  defp arith(op, a, b) do
    case op do
      :add -> inf_add(a, b)
      :sub -> inf_add(a, inf_negate(b))
      :mul -> inf_mul(a, b)
      :div -> inf_div(a, b)
      _ -> :nan
    end
  end

  defp inf_negate(:inf), do: :neg_inf
  defp inf_negate(:neg_inf), do: :inf
  defp inf_negate(v), do: -v

  defp inf_add(:inf, :neg_inf), do: :nan
  defp inf_add(:neg_inf, :inf), do: :nan
  defp inf_add(:inf, _), do: :inf
  defp inf_add(_, :inf), do: :inf
  defp inf_add(:neg_inf, _), do: :neg_inf
  defp inf_add(_, :neg_inf), do: :neg_inf

  defp inf_mul(a, b) do
    cond do
      a == 0 or b == 0 -> :nan
      inf_sign(a) * inf_sign(b) > 0 -> :inf
      true -> :neg_inf
    end
  end

  defp inf_div(a, b) when a in [:inf, :neg_inf] and b in [:inf, :neg_inf], do: :nan

  defp inf_div(a, b) when a in [:inf, :neg_inf] do
    if inf_sign(a) * inf_sign(b) > 0, do: :inf, else: :neg_inf
  end

  defp inf_div(_a, b) when b in [:inf, :neg_inf], do: 0.0

  defp inf_sign(:inf), do: 1
  defp inf_sign(:neg_inf), do: -1
  defp inf_sign(v) when v >= 0, do: 1
  defp inf_sign(_v), do: -1

  defp safe_div(a, b) when b == 0 do
    cond do
      a > 0 -> :inf
      a < 0 -> :neg_inf
      true -> :nan
    end
  end

  defp safe_div(a, b), do: a / b

  defp safe_pow(a, b) do
    :math.pow(a, b)
  rescue
    ArithmeticError -> if a == 0 and b < 0, do: :inf, else: :nan
  end

  defp cmp(:neq, :nan, _), do: true
  defp cmp(:neq, _, :nan), do: true
  defp cmp(_op, :nan, _), do: false
  defp cmp(_op, _, :nan), do: false

  defp cmp(op, a, b) when is_number(a) and is_number(b), do: cmp_ord(op, a, b)

  defp cmp(op, a, b), do: cmp_ord(op, inf_rank(a), inf_rank(b))

  defp cmp_ord(:eq, a, b), do: a == b
  defp cmp_ord(:neq, a, b), do: a != b
  defp cmp_ord(:gt, a, b), do: a > b
  defp cmp_ord(:lt, a, b), do: a < b
  defp cmp_ord(:gte, a, b), do: a >= b
  defp cmp_ord(:lte, a, b), do: a <= b

  # --- series helpers ---

  defp map_values(series, fun) do
    Enum.map(series, fn %{data: data} = s ->
      %{s | data: Enum.map(data, fn {ts, v} -> {ts, fun.(v)} end)}
    end)
  end

  defp drop_names(series) do
    Enum.map(series, fn %{labels: l} = s -> %{s | labels: Map.delete(l, "__name__")} end)
  end

  # --- windowed evaluation over raw samples ---
  #
  # Fetches raw samples for the selector over [from - window, to]
  # (offset-shifted), then evaluates `window_fun` at each grid point T over
  # the samples in (T - window, T]. window_fun receives (slice, prev) where
  # prev is the newest sample at or before T - window — the carry-in that
  # lets counter functions span the full window — and returns a value or
  # :skip (no sample emitted at that grid point).

  @default_max_samples 10_000_000

  defp eval_windowed(sel, window, keep_name, ctx, window_fun) do
    with {:ok, raw} <- fetch_raw(sel, window, keep_name, ctx),
         :ok <- check_sample_budget(raw) do
      from = ctx.from - sel.offset
      to = ctx.to - sel.offset

      series =
        raw
        |> Enum.map(fn %{labels: labels, points: points} ->
          data =
            points
            |> Enum.sort_by(&elem(&1, 0))
            |> grid_eval(from, to, ctx.step, window, window_fun)
            |> shift_data(sel.offset)

          %{labels: labels, data: data}
        end)
        |> Enum.reject(&(&1.data == []))

      {:ok, series}
    end
  end

  defp check_sample_budget(raw) do
    budget = Application.get_env(:timeless_metrics, :promql_max_samples, @default_max_samples)
    total = Enum.reduce(raw, 0, fn %{points: pts}, acc -> acc + length(pts) end)

    if total > budget do
      {:error,
       "query would process #{total} raw samples (limit #{budget}) — narrow the time range or label filters"}
    else
      :ok
    end
  end

  defp shift_data(data, 0), do: data
  defp shift_data(data, offset), do: Enum.map(data, fn {ts, v} -> {ts + offset, v} end)

  defp fetch_raw(%{pattern: nil, metric: metric} = sel, window, keep_name, ctx) do
    {:ok, results} =
      TimelessMetrics.query_multi(ctx.store, metric, sel.labels,
        from: ctx.from - sel.offset - window,
        to: ctx.to - sel.offset
      )

    {:ok,
     Enum.map(results, fn %{labels: l, points: pts} ->
       %{labels: maybe_name(l, metric, keep_name), points: pts}
     end)}
  end

  defp fetch_raw(%{pattern: pattern} = sel, window, keep_name, ctx) do
    with {:ok, regex} <- compile_anchored(pattern) do
      {:ok, all_metrics} = TimelessMetrics.list_metrics(ctx.store)

      results =
        all_metrics
        |> Enum.filter(&Regex.match?(regex, &1))
        |> Enum.flat_map(fn metric ->
          {:ok, results} =
            TimelessMetrics.query_multi(ctx.store, metric, sel.labels,
              from: ctx.from - sel.offset - window,
              to: ctx.to - sel.offset
            )

          Enum.map(results, fn %{labels: l, points: pts} ->
            %{labels: maybe_name(l, metric, keep_name), points: pts}
          end)
        end)

      {:ok, results}
    end
  end

  # Two-pointer sweep over timestamp-sorted points: lo = first index with
  # ts > T - window, hi = first index with ts > T; arr[lo..hi-1] is the
  # window slice (T - window, T], arr[lo - 1] the carry-in. O(points + steps)
  # pointer movement.
  defp grid_eval(points, from, to, step, window, window_fun) do
    arr = List.to_tuple(points)
    n = tuple_size(arr)
    do_grid_eval(Enum.to_list(from..to//step), arr, n, 0, 0, window, window_fun, [])
  end

  defp do_grid_eval([], _arr, _n, _lo, _hi, _window, _fun, acc), do: Enum.reverse(acc)

  defp do_grid_eval([t | rest], arr, n, lo, hi, window, fun, acc) do
    hi = advance_index(arr, n, hi, t)
    lo = advance_index(arr, n, lo, t - window)

    slice = slice_range(arr, lo, hi)
    prev = if lo > 0, do: elem(arr, lo - 1), else: nil

    acc =
      case fun.(slice, prev, t) do
        :skip -> acc
        v -> [{t, v} | acc]
      end

    do_grid_eval(rest, arr, n, lo, hi, window, fun, acc)
  end

  defp advance_index(arr, n, idx, bound) do
    cond do
      idx >= n -> idx
      elem(elem(arr, idx), 0) <= bound -> advance_index(arr, n, idx + 1, bound)
      true -> idx
    end
  end

  defp slice_range(_arr, lo, hi) when hi <= lo, do: []
  defp slice_range(arr, lo, hi), do: for(i <- lo..(hi - 1), do: elem(arr, i))

  # --- window functions ---

  defp window_last([], _prev, _t), do: :skip
  defp window_last(slice, _prev, _t), do: slice |> List.last() |> elem(1)

  defp rollup_window_fun(f, window) do
    case f do
      :avg_over_time ->
        stat_fun(fn vals -> Enum.sum(vals) / length(vals) end)

      :min_over_time ->
        stat_fun(&Enum.min/1)

      :max_over_time ->
        stat_fun(&Enum.max/1)

      :sum_over_time ->
        stat_fun(fn vals -> Enum.sum(vals) * 1.0 end)

      :count_over_time ->
        stat_fun(fn vals -> length(vals) * 1.0 end)

      :stddev_over_time ->
        stat_fun(fn vals -> :math.sqrt(window_variance(vals)) end)

      :stdvar_over_time ->
        stat_fun(&window_variance/1)

      :present_over_time ->
        stat_fun(fn _vals -> 1.0 end)

      :last_over_time ->
        &window_last/3

      :first_over_time ->
        fn
          [], _prev, _t -> :skip
          slice, _prev, _t -> slice |> hd() |> elem(1)
        end

      :rate ->
        fn slice, prev, _t -> counter_rate(slice, prev, window) end

      :increase ->
        fn slice, prev, _t -> counter_increase(slice, prev) end

      :irate ->
        fn slice, prev, _t -> instant_rate(slice, prev) end

      :delta ->
        fn slice, prev, _t -> gauge_delta(slice, prev) end

      :idelta ->
        fn slice, prev, _t -> gauge_idelta(slice, prev) end

      :deriv ->
        fn
          slice, _prev, t when length(slice) >= 2 ->
            {slope, _intercept} = linear_regression(slice, t)
            slope

          [_single], _prev, _t ->
            0.0

          [], _prev, _t ->
            :skip
        end

      :changes ->
        pairwise_count_fun(fn v1, v2 -> v2 != v1 end)

      :resets ->
        pairwise_count_fun(fn v1, v2 -> v2 < v1 end)
    end
  end

  # Count qualifying adjacent pairs over [prev | slice] — VM includes the
  # transition from the carry-in sample (implicit zero at a series head).
  defp pairwise_count_fun(pred) do
    fn
      [], _prev, _t ->
        :skip

      slice, prev, _t ->
        slice
        |> seq_with_prev(prev)
        |> Enum.chunk_every(2, 1, :discard)
        |> Enum.count(fn [{_t1, v1}, {_t2, v2}] -> pred.(v1, v2) end)
        |> Kernel.*(1.0)
    end
  end

  defp window_variance(vals) do
    n = length(vals)
    mean = Enum.sum(vals) / n
    Enum.reduce(vals, 0.0, fn v, acc -> acc + (v - mean) * (v - mean) end) / n
  end

  # Signed gauge difference over [carry-in | window]. At a series head VM
  # (rollupDelta) counts from an implicit zero only when the first value is
  # small relative to the first adjacent delta — a heuristic distinguishing
  # "new counter born at ~0" from "gauge that was already large".
  defp gauge_delta([], _prev), do: :skip

  defp gauge_delta(slice, nil) do
    {_t, v_first} = hd(slice)
    {_t2, v_last} = List.last(slice)

    d =
      case slice do
        [{_ta, va}, {_tb, vb} | _] -> vb - va
        _ -> 0.0
      end

    if abs(v_first) <= 10 * (abs(d) + 1) do
      v_last - 0.0
    else
      v_last - v_first
    end
  end

  defp gauge_delta(slice, prev) do
    {_t1, v_first} = prev
    {_t2, v_last} = List.last(slice)
    v_last - v_first
  end

  defp gauge_idelta(slice, prev) do
    case Enum.take(seq_with_prev(slice, prev), -2) do
      [{_t1, v1}, {_t2, v2}] -> v2 - v1
      _ -> :skip
    end
  end

  # Least-squares fit over the window slice with x = ts - t_ref.
  # Returns {slope_per_second, intercept_at_t_ref}.
  defp linear_regression(slice, t_ref) do
    n = length(slice)

    {sx, sy, sxy, sxx} =
      Enum.reduce(slice, {0.0, 0.0, 0.0, 0.0}, fn {ts, v}, {sx, sy, sxy, sxx} ->
        x = (ts - t_ref) * 1.0
        {sx + x, sy + v, sxy + x * v, sxx + x * x}
      end)

    denom = n * sxx - sx * sx

    if denom == 0 do
      {:nan, :nan}
    else
      slope = (n * sxy - sx * sy) / denom
      intercept = (sy - slope * sx) / n
      {slope, intercept}
    end
  end

  defp stat_fun(fun) do
    fn
      [], _prev, _t -> :skip
      slice, _prev, _t -> slice |> Enum.map(&elem(&1, 1)) |> fun.()
    end
  end

  # VM treats a series head (no sample before the window) as growth from an
  # implicit zero — the documented VM increase()-counts-the-first-value
  # behavior, verified via scripts/vm_diff.exs. Applies to increase, delta,
  # idelta, and changes; NOT to irate (which stays absent at a lone sample).
  defp seq_with_prev([], _prev), do: []
  defp seq_with_prev(slice, nil), do: [{elem(hd(slice), 0) - 1, 0.0} | slice]
  defp seq_with_prev(slice, prev), do: [prev | slice]

  # Reset-adjusted increase over [prev | slice] (VM-style: the carry-in
  # sample makes the increase span the full window, without Prometheus'
  # extrapolation). On a counter reset the post-reset value is the delta.
  defp counter_increase(slice, prev) do
    case seq_with_prev(slice, prev) do
      [] ->
        :skip

      seq ->
        seq
        |> Enum.chunk_every(2, 1, :discard)
        |> Enum.reduce(0.0, fn [{_t1, v1}, {_t2, v2}], acc ->
          acc + if v2 >= v1, do: v2 - v1, else: v2
        end)
    end
  end

  # With carry-in the increase spans the full window; without it (series
  # head) VM divides by the actual data span inside the window. A lone
  # sample has zero span — no rate.
  defp counter_rate(slice, prev, window) do
    case counter_increase(slice, prev) do
      :skip ->
        :skip

      inc when prev != nil ->
        inc / window

      inc ->
        {t_first, _} = hd(slice)
        {t_last, _} = List.last(slice)
        if t_last > t_first, do: inc / (t_last - t_first), else: :skip
    end
  end

  # irate: slope of the last two samples in the window (reset-aware)
  defp instant_rate(slice, prev) do
    seq = if prev, do: [prev | slice], else: slice

    case Enum.take(seq, -2) do
      [{t1, v1}, {t2, v2}] when t2 > t1 ->
        dv = if v2 >= v1, do: v2 - v1, else: v2
        dv / (t2 - t1)

      _ ->
        :skip
    end
  end

  defp compile_anchored(pattern) do
    case Regex.compile("^(?:" <> pattern <> ")$") do
      {:ok, regex} -> {:ok, regex}
      {:error, _} -> {:error, "invalid regex in __name__ matcher: #{inspect(pattern)}"}
    end
  end

  defp maybe_name(labels, metric, true) when is_binary(metric),
    do: Map.put(labels, "__name__", metric)

  defp maybe_name(labels, _metric, _keep), do: labels

  # --- response formatting ---

  defp format_series(series) do
    series
    |> Enum.sort_by(& &1.labels)
    |> Enum.map(fn %{labels: l, data: data} ->
      %{
        "metric" => l,
        "values" => Enum.map(data, fn {ts, val} -> [ts, format_value(val)] end)
      }
    end)
  end

  defp wrap_prom_response(results) do
    %{
      "status" => "success",
      "data" => %{
        "resultType" => "matrix",
        "result" => results
      }
    }
  end

  defp format_value(:inf), do: "+Inf"
  defp format_value(:neg_inf), do: "-Inf"
  defp format_value(:nan), do: "NaN"
  defp format_value(val) when is_float(val), do: Float.to_string(val)
  defp format_value(val) when is_integer(val), do: Float.to_string(val / 1)
end
