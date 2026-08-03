defmodule TimelessMetrics.LegacyReader do
  @moduledoc false

  alias TimelessMetrics.RustEngine.Nif

  @page_limit 4_096

  defstruct [:resource, :root]

  @type t :: %__MODULE__{resource: reference(), root: String.t()}

  @spec open(String.t()) :: {:ok, t()} | {:error, String.t()}
  def open(root) do
    root = Path.expand(root)

    case normalize(Nif.engine_legacy_open(root)) do
      {:error, reason} -> {:error, to_string(reason)}
      {:ok, resource} -> {:ok, %__MODULE__{resource: resource, root: root}}
    end
  rescue
    error -> {:error, Exception.message(error)}
  end

  def series(%__MODULE__{resource: resource}) do
    with {:ok, rows} <- normalize(Nif.engine_legacy_list_series(resource)) do
      {:ok, Enum.map(rows, fn {metric, labels} -> {metric, Map.new(labels)} end)}
    end
  end

  def info(%__MODULE__{resource: resource}) do
    resource
    |> Nif.engine_legacy_info()
    |> normalize()
  end

  def page(%__MODULE__{resource: resource}, metric, labels, cursor, limit \\ @page_limit)
      when limit in 1..@page_limit do
    case normalize(Nif.engine_legacy_query_page(resource, metric, Map.new(labels), cursor, limit)) do
      {:ok, {points, next_cursor, has_more?}} -> {:ok, points, next_cursor, has_more?}
      {:error, _} = error -> error
    end
  end

  defp normalize(value), do: TimelessMetrics.RustEngine.normalize_nif_result(value)
end
