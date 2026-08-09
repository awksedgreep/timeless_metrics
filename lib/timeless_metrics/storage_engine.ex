defmodule TimelessMetrics.StorageEngine do
  @moduledoc false

  for {name, arity} <- [
        write: 5,
        write_batch: 2,
        resolve_series: 3,
        write_resolved: 4,
        ingest_prometheus: 2,
        query_raw: 4,
        query_multi: 4,
        query_aggregate: 4,
        query_aggregate_multi: 4,
        latest: 3,
        latest_multi: 3,
        flush: 1,
        compact: 1,
        info: 1,
        delete_before: 2,
        list_metrics: 1,
        list_series: 2,
        label_values: 3,
        find_series: 3,
        rollup: 1
      ] do
    args = Macro.generate_arguments(arity, __MODULE__)
    [store | _] = args

    def unquote(name)(unquote_splicing(args)) do
      apply(backend(unquote(store)), unquote(name), [unquote_splicing(args)])
    end
  end

  def backend(store) do
    case :persistent_term.get({TimelessMetrics, store, :engine}, :libsql) do
      :rust -> TimelessMetrics.RustEngine
      _ -> TimelessMetrics.LibsqlEngine
    end
  end
end
