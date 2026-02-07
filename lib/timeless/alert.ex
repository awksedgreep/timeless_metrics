defmodule Timeless.Alert do
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
    labels = Keyword.get(opts, :labels, %{}) |> Jason.encode!()
    condition = to_string(Keyword.fetch!(opts, :condition))
    threshold = Keyword.fetch!(opts, :threshold) * 1.0
    duration = Keyword.get(opts, :duration, 0)
    aggregate = to_string(Keyword.get(opts, :aggregate, :avg))
    webhook_url = Keyword.get(opts, :webhook_url)
    created_at = System.os_time(:second)

    {:ok, id} =
      Timeless.DB.write_transaction(db, fn conn ->
        Timeless.DB.execute(
          conn,
          """
          INSERT INTO alert_rules (name, metric, labels, condition, threshold, duration, aggregate, webhook_url, enabled, created_at)
          VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1, ?9)
          """,
          [name, metric, labels, condition, threshold, duration, aggregate, webhook_url, created_at]
        )

        {:ok, [[id]]} = Timeless.DB.execute(conn, "SELECT last_insert_rowid()", [])
        id
      end)

    {:ok, id}
  end

  @doc """
  List all alert rules with their current state.
  """
  def list_rules(db) do
    {:ok, rows} =
      Timeless.DB.read(
        db,
        "SELECT id, name, metric, labels, condition, threshold, duration, aggregate, webhook_url, enabled FROM alert_rules ORDER BY id"
      )

    rules =
      Enum.map(rows, fn [id, name, metric, labels, condition, threshold, duration, aggregate, webhook_url, enabled] ->
        # Get current state for this rule
        {:ok, state_rows} =
          Timeless.DB.read(
            db,
            "SELECT series_labels, state, triggered_at, last_value FROM alert_state WHERE rule_id = ?1",
            [id]
          )

        states =
          Enum.map(state_rows, fn [series_labels, state, triggered_at, last_value] ->
            %{
              series_labels: Jason.decode!(series_labels),
              state: state,
              triggered_at: triggered_at,
              last_value: last_value
            }
          end)

        %{
          id: id,
          name: name,
          metric: metric,
          labels: Jason.decode!(labels || "{}"),
          condition: condition,
          threshold: threshold,
          duration: duration,
          aggregate: aggregate,
          webhook_url: webhook_url,
          enabled: enabled == 1,
          states: states
        }
      end)

    {:ok, rules}
  end

  @doc """
  Delete an alert rule and its state.
  """
  def delete_rule(db, rule_id) do
    Timeless.DB.write(db, "DELETE FROM alert_state WHERE rule_id = ?1", [rule_id])
    Timeless.DB.write(db, "DELETE FROM alert_rules WHERE id = ?1", [rule_id])
    :ok
  end

  @doc """
  Evaluate all enabled alert rules against current data.

  Called from the rollup tick or manually.
  """
  def evaluate(store) do
    db = :"#{store}_db"

    {:ok, rules} = list_rules(db)

    Enum.each(rules, fn rule ->
      if rule.enabled do
        evaluate_rule(store, db, rule)
      end
    end)
  end

  defp evaluate_rule(store, db, rule) do
    now = System.os_time(:second)
    lookback = max(rule.duration, 600)
    label_filter = rule.labels
    agg = String.to_existing_atom(rule.aggregate)

    {:ok, results} =
      Timeless.query_aggregate_multi(store, rule.metric, label_filter,
        from: now - lookback,
        to: now,
        bucket: {lookback, :seconds},
        aggregate: agg
      )

    Enum.each(results, fn %{labels: labels, data: buckets} ->
      value =
        case buckets do
          [] -> nil
          _ ->
            {_ts, val} = List.last(buckets)
            val
        end

      if value != nil do
        breaching = check_condition(value, rule.condition, rule.threshold)
        series_key = Jason.encode!(labels)
        update_state(db, rule, series_key, labels, value, breaching, now)
      end
    end)
  end

  defp check_condition(value, "above", threshold), do: value > threshold
  defp check_condition(value, "below", threshold), do: value < threshold

  defp update_state(db, rule, series_key, labels, value, breaching, now) do
    {:ok, current_rows} =
      Timeless.DB.read(
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

    resolved_at = if new_state == "resolved", do: now, else: nil

    Timeless.DB.write(
      db,
      """
      INSERT OR REPLACE INTO alert_state (rule_id, series_labels, state, triggered_at, resolved_at, last_value)
      VALUES (?1, ?2, ?3, ?4, ?5, ?6)
      """,
      [rule.id, series_key, new_state, triggered_at, resolved_at, value]
    )

    if should_notify && rule.webhook_url do
      deliver_webhook(rule, labels, value, new_state, now)
    end
  end

  defp deliver_webhook(rule, labels, value, state, timestamp) do
    label_params =
      labels
      |> Enum.map(fn {k, v} -> "#{k}=#{v}" end)
      |> Enum.join("&")

    chart_url = "chart?metric=#{rule.metric}&#{label_params}&from=-1h&theme=auto"

    payload =
      Jason.encode!(%{
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
      })

    # Fire and forget — don't block the alert loop on webhook delivery
    Task.start(fn ->
      case :httpc.request(
             :post,
             {String.to_charlist(rule.webhook_url), [], ~c"application/json", payload},
             [{:timeout, 10_000}],
             []
           ) do
        {:ok, _} ->
          :ok

        {:error, reason} ->
          Logger.warning("Alert webhook failed for #{rule.name}: #{inspect(reason)}")
      end
    end)
  end
end
