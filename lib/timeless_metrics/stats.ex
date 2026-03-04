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

  @counter_size 8

  @doc "Initialize counters for a store. Call once before children start."
  def init(store) do
    ref = :counters.new(@counter_size, [:write_concurrency])
    :persistent_term.put({__MODULE__, store}, ref)
  end

  # --- Increment by 1 ---

  def incr_writes(store), do: add(store, @writes_total, 1)
  def incr_series_created(store), do: add(store, @series_created, 1)
  def incr_http_imports(store), do: add(store, @http_imports, 1)
  def incr_http_queries(store), do: add(store, @http_queries, 1)
  def incr_merges_completed(store), do: add(store, @merges_completed, 1)

  # --- Add N ---

  def add_points(store, n), do: add(store, @points_ingested, n)
  def add_http_import_errors(_store, n) when n <= 0, do: :ok
  def add_http_import_errors(store, n), do: add(store, @http_import_errors, n)
  def add_points_merged(store, n), do: add(store, @points_merged, n)

  # --- Snapshot ---

  @doc "Read all 8 counters as a map."
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
            :points_merged
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
          points_merged: :counters.get(ref, @points_merged)
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
