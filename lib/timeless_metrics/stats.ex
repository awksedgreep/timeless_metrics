defmodule TimelessMetrics.Stats do
  @moduledoc false

  # Counter indices
  @writes_total 1
  @points_ingested 2
  @series_created 3
  @http_imports 4
  @http_queries 5
  @http_import_errors 6
  @merges_completed 7
  @points_merged 8
  @queries 9
  @query_fast_path 10
  @query_slow_path 11
  @promql_rejected 12
  @http_batches_admitted 13
  @http_batches_completed 14

  @counter_size 14

  # Bounded sample of recently rejected PromQL queries (the "gap radar"):
  # real traffic tells us which unsupported constructs to implement next.
  @max_rejection_samples 100

  @doc "Initialize counters for a store. Call once before children start."
  def init(store) do
    ref = :counters.new(@counter_size, [:write_concurrency])
    :persistent_term.put({__MODULE__, store}, ref)

    rejections = rejection_table_name(store)

    if :ets.whereis(rejections) == :undefined do
      :ets.new(rejections, [:named_table, :set, :public, write_concurrency: :auto])
    end

    :ok
  end

  # --- Increment by 1 ---

  def incr_writes(store), do: add(store, @writes_total, 1)
  def incr_series_created(store), do: add(store, @series_created, 1)
  def incr_http_imports(store), do: add(store, @http_imports, 1)
  def incr_http_queries(store), do: add(store, @http_queries, 1)
  def incr_merges_completed(store), do: add(store, @merges_completed, 1)
  def incr_queries(store), do: add(store, @queries, 1)
  def incr_query_fast_path(store), do: add(store, @query_fast_path, 1)
  def incr_query_slow_path(store), do: add(store, @query_slow_path, 1)
  def incr_http_batches_admitted(store), do: add(store, @http_batches_admitted, 1)
  def incr_http_batches_completed(store), do: add(store, @http_batches_completed, 1)

  # --- Add N ---

  def add_points(store, n), do: add(store, @points_ingested, n)
  def add_http_import_errors(_store, n) when n <= 0, do: :ok
  def add_http_import_errors(store, n), do: add(store, @http_import_errors, n)
  def add_points_merged(store, n), do: add(store, @points_merged, n)

  # --- PromQL gap radar ---

  @doc """
  Record a rejected PromQL query: bumps the counter and keeps a bounded
  sample of distinct query strings with their rejection reason.
  """
  def record_promql_rejection(store, query, reason) when is_binary(query) do
    add(store, @promql_rejected, 1)
    table = rejection_table_name(store)

    if :ets.whereis(table) != :undefined do
      now = System.os_time(:second)

      case :ets.lookup(table, query) do
        [{^query, _reason, count, _last_ts}] ->
          :ets.insert(table, {query, reason, count + 1, now})

        [] ->
          if :ets.info(table, :size) < @max_rejection_samples do
            :ets.insert(table, {query, reason, 1, now})
          end
      end
    end

    :ok
  end

  def record_promql_rejection(_store, _query, _reason), do: :ok

  @doc """
  Recently rejected PromQL queries, most recent first:
  `[%{query, reason, count, last_seen}]`.
  """
  def promql_rejections(store, limit \\ 50) do
    table = rejection_table_name(store)

    if :ets.whereis(table) == :undefined do
      []
    else
      table
      |> :ets.tab2list()
      |> Enum.sort_by(fn {_q, _r, _c, last_ts} -> -last_ts end)
      |> Enum.take(limit)
      |> Enum.map(fn {query, reason, count, last_ts} ->
        %{query: query, reason: reason, count: count, last_seen: last_ts}
      end)
    end
  end

  defp rejection_table_name(store), do: :"#{store}_promql_rejections"

  # --- Snapshot ---

  @doc "Read all counters as a map."
  def snapshot(store) do
    case ref(store) do
      nil ->
        Map.new(
          [
            :writes_total,
            :points_ingested,
            :series_created,
            :http_imports,
            :http_queries,
            :http_import_errors,
            :merges_completed,
            :points_merged,
            :queries,
            :query_fast_path,
            :query_slow_path,
            :promql_rejected,
            :http_batches_admitted,
            :http_batches_completed
          ],
          &{&1, 0}
        )

      ref ->
        %{
          writes_total: :counters.get(ref, @writes_total),
          points_ingested: :counters.get(ref, @points_ingested),
          series_created: :counters.get(ref, @series_created),
          http_imports: :counters.get(ref, @http_imports),
          http_queries: :counters.get(ref, @http_queries),
          http_import_errors: :counters.get(ref, @http_import_errors),
          merges_completed: :counters.get(ref, @merges_completed),
          points_merged: :counters.get(ref, @points_merged),
          queries: :counters.get(ref, @queries),
          query_fast_path: :counters.get(ref, @query_fast_path),
          query_slow_path: :counters.get(ref, @query_slow_path),
          promql_rejected: :counters.get(ref, @promql_rejected),
          http_batches_admitted: :counters.get(ref, @http_batches_admitted),
          http_batches_completed: :counters.get(ref, @http_batches_completed)
        }
    end
  end

  defp add(store, index, n) do
    case ref(store) do
      nil -> :ok
      ref -> :counters.add(ref, index, n)
    end
  end

  defp ref(store) do
    :persistent_term.get({__MODULE__, store}, nil)
  end
end
