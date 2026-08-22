defmodule TimelessMetrics.Alert do
  @moduledoc """
  Alert engine that evaluates threshold rules against metric data.

  Piggybacks on the rollup tick interval — no extra timer. Call `evaluate/1`
  from the rollup tick or manually.

  State transitions: ok → pending → firing → resolved
  Delivers structured JSON webhooks on firing and resolved transitions.
  """

  require Logger

  @doc """
  Create an alert rule.

  Returns `{:ok, id}`.
  """
  def create_rule(db, opts) do
    name = Keyword.fetch!(opts, :name)
    metric = Keyword.fetch!(opts, :metric)
    labels = Keyword.get(opts, :labels, %{}) |> :json.encode() |> IO.iodata_to_binary()
    condition = to_string(Keyword.fetch!(opts, :condition))
    threshold = Keyword.fetch!(opts, :threshold) * 1.0
    duration = Keyword.get(opts, :duration, 0)
    aggregate = to_string(Keyword.get(opts, :aggregate, :avg))
    webhook_url = Keyword.get(opts, :webhook_url)
    webhook_format = normalize_format(Keyword.get(opts, :webhook_format))
    created_at = System.os_time(:second)

    {:ok, id} =
      TimelessMetrics.DB.write_transaction(db, fn conn ->
        TimelessMetrics.DB.execute(
          conn,
          """
          INSERT INTO alert_rules (name, metric, labels, condition, threshold, duration, aggregate, webhook_url, webhook_format, enabled, created_at)
          VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1, ?10)
          """,
          [
            name,
            metric,
            labels,
            condition,
            threshold,
            duration,
            aggregate,
            webhook_url,
            webhook_format,
            created_at
          ]
        )

        {:ok, [[id]]} = TimelessMetrics.DB.execute(conn, "SELECT last_insert_rowid()", [])
        id
      end)

    {:ok, id}
  end

  @doc """
  List all alert rules with their current state.
  """
  def list_rules(db) do
    {:ok, rows} =
      TimelessMetrics.DB.read(
        db,
        "SELECT id, name, metric, labels, condition, threshold, duration, aggregate, webhook_url, webhook_format, enabled FROM alert_rules ORDER BY id"
      )

    rules =
      Enum.map(rows, fn [
                          id,
                          name,
                          metric,
                          labels,
                          condition,
                          threshold,
                          duration,
                          aggregate,
                          webhook_url,
                          webhook_format,
                          enabled
                        ] ->
        # Get current state for this rule
        {:ok, state_rows} =
          TimelessMetrics.DB.read(
            db,
            "SELECT series_labels, state, triggered_at, last_value FROM alert_state WHERE rule_id = ?1",
            [id]
          )

        states =
          Enum.map(state_rows, fn [series_labels, state, triggered_at, last_value] ->
            %{
              series_labels: :json.decode(series_labels),
              state: state,
              triggered_at: triggered_at,
              last_value: last_value
            }
          end)

        %{
          id: id,
          name: name,
          metric: metric,
          labels: :json.decode(labels || "{}"),
          condition: condition,
          threshold: threshold,
          duration: duration,
          aggregate: aggregate,
          webhook_url: webhook_url,
          webhook_format: webhook_format,
          enabled: enabled == 1,
          states: states
        }
      end)

    {:ok, rules}
  end

  @doc """
  Update an alert rule (partial update).

  Supported fields: name, metric, labels, condition, threshold, duration, aggregate, webhook_url, webhook_format, enabled.
  Only provided fields are updated.

  Returns `:ok`.
  """
  @updatable_fields ~w(name metric labels condition threshold duration aggregate webhook_url webhook_format enabled)

  def update_rule(db, rule_id, opts) do
    fields =
      opts
      |> Enum.filter(fn {k, _v} -> to_string(k) in @updatable_fields end)
      |> Enum.map(fn {k, v} ->
        key = to_string(k)

        val =
          case key do
            "labels" -> :json.encode(v) |> IO.iodata_to_binary()
            "condition" -> to_string(v)
            "aggregate" -> to_string(v)
            "threshold" -> v * 1.0
            "enabled" -> if(v, do: 1, else: 0)
            "webhook_format" -> normalize_format(v)
            _ -> v
          end

        {key, val}
      end)

    if fields == [] do
      :ok
    else
      set_clause =
        fields |> Enum.with_index(1) |> Enum.map_join(", ", fn {{k, _v}, i} -> "#{k} = ?#{i}" end)

      values = Enum.map(fields, fn {_k, v} -> v end) ++ [rule_id]
      id_placeholder = "?#{length(fields) + 1}"

      TimelessMetrics.DB.write(
        db,
        "UPDATE alert_rules SET #{set_clause} WHERE id = #{id_placeholder}",
        values
      )

      # Clear alert state when disabling so re-enable starts fresh
      if Enum.any?(fields, fn {k, v} -> k == "enabled" and v == 0 end) do
        TimelessMetrics.DB.write(
          db,
          "DELETE FROM alert_state WHERE rule_id = ?1",
          [rule_id]
        )
      end

      :ok
    end
  end

  @doc """
  Delete an alert rule and its state.
  """
  def delete_rule(db, rule_id) do
    TimelessMetrics.DB.write(db, "DELETE FROM alert_state WHERE rule_id = ?1", [rule_id])
    TimelessMetrics.DB.write(db, "DELETE FROM alert_rules WHERE id = ?1", [rule_id])
    :ok
  end

  @doc """
  Evaluate all enabled alert rules against current data.

  Called from the rollup tick or manually.
  """
  def evaluate(store) do
    db = :"#{store}_db"

    {:ok, rules} = list_rules(db)

    rules
    |> Enum.filter(& &1.enabled)
    |> Enum.each(&safely_evaluate(store, db, &1))

    :ok
  rescue
    # list_rules/1 raises rather than returning an error when the rules table
    # is unreachable. Letting that propagate would restart the evaluator on a
    # loop and stop alerting entirely, silently.
    error ->
      Logger.error("alert evaluation skipped: cannot read rules: " <> Exception.message(error))
      {:error, error}
  end

  # One rule must not be able to end the pass. A metric that no longer exists,
  # or a reader that is briefly unreachable, would otherwise raise out of
  # Enum.each and — under the evaluator's supervisor — turn into a restart
  # loop that stops evaluating every OTHER rule too. Alerting that has quietly
  # stopped looks exactly like alerting with nothing to report.
  defp safely_evaluate(store, db, rule) do
    evaluate_rule(store, db, rule)
  rescue
    error ->
      Logger.error(
        "alert rule #{rule.id} (#{rule.name}) failed to evaluate: " <>
          Exception.message(error)
      )

      :error
  catch
    kind, reason ->
      Logger.error(
        "alert rule #{rule.id} (#{rule.name}) failed to evaluate: #{inspect({kind, reason})}"
      )

      :error
  end

  defp evaluate_rule(store, db, rule) do
    now = System.os_time(:second)
    lookback = max(rule.duration, 600)
    label_filter = rule.labels
    agg = String.to_existing_atom(rule.aggregate)

    {:ok, results} =
      reader().query_aggregate_multi(store, rule.metric, label_filter,
        from: now - lookback,
        to: now,
        bucket: {lookback, :seconds},
        aggregate: agg
      )

    Enum.each(results, fn %{labels: labels, data: buckets} ->
      value =
        case buckets do
          [] ->
            nil

          _ ->
            {_ts, val} = List.last(buckets)
            val
        end

      if value != nil do
        breaching = check_condition(value, rule.condition, rule.threshold)
        series_key = :json.encode(labels) |> IO.iodata_to_binary()
        update_state(db, rule, series_key, labels, value, breaching, now)
      end
    end)
  end

  # Where rule evaluation reads metrics from. Defaults to this library's own
  # store; a deployment whose metrics live behind a data plane (owner:
  # :external, where no in-process store exists) points this at an adapter
  # exposing the same query_aggregate_multi/4.
  defp reader do
    Application.get_env(:timeless_metrics, :alert_reader, TimelessMetrics)
  end

  defp check_condition(value, "above", threshold), do: value > threshold
  defp check_condition(value, "below", threshold), do: value < threshold

  defp update_state(db, rule, series_key, labels, value, breaching, now) do
    {:ok, current_rows} =
      TimelessMetrics.DB.read(
        db,
        "SELECT state, triggered_at FROM alert_state WHERE rule_id = ?1 AND series_labels = ?2",
        [rule.id, series_key]
      )

    current_state =
      case current_rows do
        [[state, triggered_at]] -> {state, triggered_at}
        [] -> {"ok", nil}
      end

    {new_state, should_notify} =
      case {current_state, breaching} do
        {{"ok", _}, true} ->
          if rule.duration == 0 do
            {"firing", :fire}
          else
            {"pending", nil}
          end

        {{"ok", _}, false} ->
          {"ok", nil}

        {{"pending", triggered_at}, true} when is_integer(triggered_at) ->
          if now - triggered_at >= rule.duration do
            {"firing", :fire}
          else
            {"pending", nil}
          end

        {{"pending", _}, true} ->
          {"pending", nil}

        {{"pending", _}, false} ->
          {"ok", nil}

        {{"firing", _}, true} ->
          {"firing", nil}

        {{"firing", _}, false} ->
          {"resolved", :resolve}

        {{"resolved", _}, true} ->
          if rule.duration == 0 do
            {"firing", :fire}
          else
            {"pending", nil}
          end

        {{"resolved", _}, false} ->
          {"ok", nil}
      end

    triggered_at =
      case {new_state, elem(current_state, 1)} do
        {"pending", nil} -> now
        {"firing", nil} -> now
        {_, existing} -> existing
      end

    # Clean up state rows that return to ok — no need to persist the default state
    if new_state == "ok" and elem(current_state, 0) != "ok" do
      TimelessMetrics.DB.write(
        db,
        "DELETE FROM alert_state WHERE rule_id = ?1 AND series_labels = ?2",
        [rule.id, series_key]
      )
    else
      if new_state != "ok" do
        resolved_at = if new_state == "resolved", do: now, else: nil

        TimelessMetrics.DB.write(
          db,
          """
          INSERT OR REPLACE INTO alert_state (rule_id, series_labels, state, triggered_at, resolved_at, last_value)
          VALUES (?1, ?2, ?3, ?4, ?5, ?6)
          """,
          [rule.id, series_key, new_state, triggered_at, resolved_at, value]
        )
      end
    end

    if should_notify do
      log_history(db, rule, series_key, value, should_notify, now)

      if rule.webhook_url do
        deliver_webhook(rule, labels, value, new_state, now)
      end
    end
  end

  defp log_history(db, rule, series_key, value, :fire, now) do
    TimelessMetrics.DB.write(
      db,
      """
      INSERT INTO alert_history (rule_id, rule_name, metric, series_labels, state, value, threshold, condition, triggered_at, created_at)
      VALUES (?1, ?2, ?3, ?4, 'firing', ?5, ?6, ?7, ?8, ?8)
      """,
      [rule.id, rule.name, rule.metric, series_key, value, rule.threshold, rule.condition, now]
    )
  end

  defp log_history(db, rule, series_key, value, :resolve, now) do
    TimelessMetrics.DB.write(
      db,
      """
      INSERT INTO alert_history (rule_id, rule_name, metric, series_labels, state, value, threshold, condition, triggered_at, resolved_at, created_at)
      VALUES (?1, ?2, ?3, ?4, 'resolved', ?5, ?6, ?7, ?8, ?8, ?8)
      """,
      [rule.id, rule.name, rule.metric, series_key, value, rule.threshold, rule.condition, now]
    )
  end

  @doc """
  List recent alert history entries, newest first.

  Options:
    - `:limit` - max entries (default 50)
    - `:rule_id` - filter by rule ID
    - `:acknowledged` - filter: true, false, or nil (all)
  """
  def list_history(db, opts \\ []) do
    limit = Keyword.get(opts, :limit, 50)
    rule_id = Keyword.get(opts, :rule_id)
    acknowledged = Keyword.get(opts, :acknowledged)

    {where_clauses, params} = build_history_filters(rule_id, acknowledged)

    where = if where_clauses == [], do: "", else: "WHERE " <> Enum.join(where_clauses, " AND ")

    {:ok, rows} =
      TimelessMetrics.DB.read(
        db,
        """
        SELECT id, rule_id, rule_name, metric, series_labels, state, value, threshold, condition, triggered_at, resolved_at, acknowledged, created_at
        FROM alert_history
        #{where}
        ORDER BY created_at DESC
        LIMIT ?#{length(params) + 1}
        """,
        params ++ [limit]
      )

    entries =
      Enum.map(rows, fn [
                          id,
                          rule_id,
                          rule_name,
                          metric,
                          series_labels,
                          state,
                          value,
                          threshold,
                          condition,
                          triggered_at,
                          resolved_at,
                          ack,
                          created_at
                        ] ->
        %{
          id: id,
          rule_id: rule_id,
          rule_name: rule_name,
          metric: metric,
          series_labels: :json.decode(series_labels),
          state: state,
          value: value,
          threshold: threshold,
          condition: condition,
          triggered_at: triggered_at,
          resolved_at: resolved_at,
          acknowledged: ack == 1,
          created_at: created_at
        }
      end)

    {:ok, entries}
  end

  defp build_history_filters(rule_id, acknowledged) do
    {clauses, params, _idx} =
      [{rule_id, "rule_id"}, {acknowledged, "acknowledged"}]
      |> Enum.reduce({[], [], 1}, fn
        {nil, _col}, acc ->
          acc

        {val, col}, {clauses, params, idx} ->
          db_val = if is_boolean(val), do: if(val, do: 1, else: 0), else: val
          {clauses ++ ["#{col} = ?#{idx}"], params ++ [db_val], idx + 1}
      end)

    {clauses, params}
  end

  @doc """
  Acknowledge a history entry by ID.
  """
  def acknowledge_alert(db, history_id) do
    TimelessMetrics.DB.write(
      db,
      "UPDATE alert_history SET acknowledged = 1 WHERE id = ?1",
      [history_id]
    )

    :ok
  end

  @doc """
  Clear history entries.

  Options:
    - `:acknowledged_only` - only delete acknowledged entries (default true)
    - `:before` - delete entries older than this timestamp (default: 7 days ago)
  """
  def clear_history(db, opts \\ []) do
    acknowledged_only = Keyword.get(opts, :acknowledged_only, true)
    before = Keyword.get(opts, :before, System.os_time(:second) - 7 * 86_400)

    if acknowledged_only do
      TimelessMetrics.DB.write(
        db,
        "DELETE FROM alert_history WHERE acknowledged = 1 AND created_at < ?1",
        [before]
      )
    else
      TimelessMetrics.DB.write(
        db,
        "DELETE FROM alert_history WHERE created_at < ?1",
        [before]
      )
    end

    :ok
  end

  defp deliver_webhook(rule, labels, value, state, timestamp) do
    label_params =
      labels
      |> Enum.map(fn {k, v} -> "#{k}=#{v}" end)
      |> Enum.join("&")

    chart_url = "chart?metric=#{rule.metric}&#{label_params}&from=-1h&theme=auto"

    {url, body} = build_delivery(rule, labels, value, state, timestamp, chart_url)
    payload = body |> :json.encode() |> IO.iodata_to_binary()

    # Fire and forget — don't block the alert loop on webhook delivery
    Task.start(fn ->
      case :httpc.request(
             :post,
             {String.to_charlist(url), [], ~c"application/json", payload},
             [{:timeout, 10_000}],
             []
           ) do
        {:ok, {{_version, status, _reason}, _headers, _body}} when status in 200..299 ->
          :ok

        # :httpc returns {:ok, _} for ANY completed exchange, so a rejected delivery
        # (429 from a rate-limited notification service, 5xx, a stale 404 endpoint)
        # looks identical to success unless the status is inspected. An alert nobody
        # receives is the failure mode operators are least able to detect on their own,
        # so this is logged at :error rather than :warning.
        {:ok, {{_version, status, _reason}, _headers, body}} ->
          Logger.error(
            "Alert webhook rejected for #{rule.name}: HTTP #{status} #{inspect(truncate(body))}"
          )

        {:error, reason} ->
          Logger.error("Alert webhook failed for #{rule.name}: #{inspect(reason)}")
      end
    end)
  end

  @webhook_formats ~w(ntfy generic)

  # Fail closed on an unrecognised format rather than silently sending the
  # generic payload somewhere that cannot read it.
  defp normalize_format(nil), do: nil

  defp normalize_format(value) do
    case to_string(value) do
      "generic" ->
        nil

      format when format in @webhook_formats ->
        format

      other ->
        raise ArgumentError,
              "invalid webhook_format #{inspect(other)}, expected one of: #{Enum.join(@webhook_formats, ", ")}"
    end
  end

  # ntfy parses a JSON body only at its **root** endpoint. Posting JSON to a
  # topic path makes the entire envelope the message text, which is why alerts
  # arrived as unreadable blobs. Translate into ntfy's own schema and post to the
  # root, carrying the topic in the body.
  @doc false
  # Public only so the payload translation can be tested directly.
  def build_delivery(rule, labels, value, state, timestamp, chart_url)

  def build_delivery(
        %{webhook_format: "ntfy"} = rule,
        labels,
        value,
        state,
        _timestamp,
        chart_url
      ) do
    {base, topic} = split_ntfy_url(rule.webhook_url)
    state = to_string(state)
    firing? = state == "firing"

    label_text =
      case Enum.map(labels, fn {k, v} -> "#{k}=#{v}" end) do
        [] -> ""
        parts -> " {" <> Enum.join(parts, ", ") <> "}"
      end

    message =
      "#{rule.metric}#{label_text} is #{format_number(value)}" <>
        " (#{rule.aggregate} #{rule.condition} #{format_number(rule.threshold)})\n#{chart_url}"

    body = %{
      topic: topic,
      title: "#{String.upcase(state)}: #{rule.name}",
      message: message,
      # Firing should be able to cut through; a recovery notice should not be
      # as loud as the outage it is clearing.
      priority: if(firing?, do: 4, else: 3),
      tags: [if(firing?, do: "rotating_light", else: "white_check_mark")]
    }

    {base, body}
  end

  def build_delivery(rule, labels, value, state, timestamp, chart_url) do
    {rule.webhook_url,
     %{
       alert: rule.name,
       metric: rule.metric,
       labels: labels,
       value: value,
       threshold: rule.threshold,
       condition: rule.condition,
       aggregate: rule.aggregate,
       state: state,
       triggered_at: timestamp,
       url: chart_url
     }}
  end

  @doc false
  # "https://ntfy.sh/my-topic" -> {"https://ntfy.sh/", "my-topic"}
  # Self-hosted under a prefix works too:
  # "https://host/ntfy/alerts" -> {"https://host/ntfy/", "alerts"}
  def split_ntfy_url(url) do
    uri = URI.parse(url)
    segments = (uri.path || "") |> String.split("/", trim: true)

    {topic, prefix} =
      case List.pop_at(segments, -1) do
        {nil, _} -> {"", []}
        {last, rest} -> {last, rest}
      end

    base =
      URI.to_string(%{
        uri
        | path: "/" <> Enum.join(prefix ++ [""], "/"),
          query: nil,
          fragment: nil
      })

    {base, topic}
  end

  defp format_number(n) when is_float(n), do: :erlang.float_to_binary(n, [:short])
  defp format_number(n), do: to_string(n)

  # Response bodies land in the log, so cap them — an HTML error page from a
  # misconfigured endpoint should not swamp the alert log.
  defp truncate(body, max \\ 200) do
    str = to_string(body)
    if byte_size(str) > max, do: binary_part(str, 0, max) <> "...", else: str
  end
end
