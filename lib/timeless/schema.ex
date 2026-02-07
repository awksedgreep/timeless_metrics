defmodule Timeless.Schema do
  @moduledoc """
  Declarative DSL for defining metric storage tiers, retention, and rollup schedules.

  ## Example

      defmodule MyApp.MetricSchema do
        use Timeless.Schema

        raw_retention {7, :days}

        tier :hourly,
          resolution: :hour,
          aggregates: [:avg, :min, :max, :count, :sum, :last],
          retention: {30, :days}

        tier :daily,
          resolution: :day,
          aggregates: [:avg, :min, :max, :count, :sum, :last],
          retention: {365, :days}

        tier :monthly,
          resolution: {30, :days},
          aggregates: [:avg, :min, :max, :count, :sum],
          retention: :forever
      end

  Then pass it to Timeless:

      {Timeless, name: :metrics, data_dir: "/data", schema: MyApp.MetricSchema}
  """

  defstruct [
    :raw_retention_seconds,
    :rollup_interval,
    :retention_interval,
    tiers: []
  ]

  defmodule Tier do
    @moduledoc false
    defstruct [:name, :resolution_seconds, :aggregates, :retention_seconds, :table_name]
  end

  @doc false
  defmacro __using__(_opts) do
    quote do
      import Timeless.Schema, only: [raw_retention: 1, tier: 2, rollup_interval: 1, retention_interval: 1]
      Module.register_attribute(__MODULE__, :tiers, accumulate: true)
      Module.put_attribute(__MODULE__, :raw_retention, {7, :days})
      Module.put_attribute(__MODULE__, :rollup_interval_ms, :timer.minutes(5))
      Module.put_attribute(__MODULE__, :retention_interval_ms, :timer.hours(1))

      @before_compile Timeless.Schema
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    tiers = Module.get_attribute(env.module, :tiers) |> Enum.reverse()
    raw_ret = Module.get_attribute(env.module, :raw_retention)
    rollup_int = Module.get_attribute(env.module, :rollup_interval_ms)
    retention_int = Module.get_attribute(env.module, :retention_interval_ms)

    quote do
      def __schema__ do
        %Timeless.Schema{
          raw_retention_seconds: Timeless.Schema.duration_to_seconds(unquote(Macro.escape(raw_ret))),
          rollup_interval: unquote(rollup_int),
          retention_interval: unquote(retention_int),
          tiers:
            unquote(
              Macro.escape(
                Enum.map(tiers, fn {name, opts} ->
                  %Timeless.Schema.Tier{
                    name: name,
                    resolution_seconds: Timeless.Schema.resolution_to_seconds(opts[:resolution]),
                    aggregates: opts[:aggregates] || [:avg, :min, :max, :count, :sum, :last],
                    retention_seconds: Timeless.Schema.duration_to_seconds(opts[:retention]),
                    table_name: "tier_#{name}"
                  }
                end)
              )
            )
        }
      end
    end
  end

  defmacro raw_retention(duration) do
    quote do
      Module.put_attribute(__MODULE__, :raw_retention, unquote(Macro.escape(duration)))
    end
  end

  defmacro tier(name, opts) do
    quote do
      Module.put_attribute(__MODULE__, :tiers, {unquote(name), unquote(Macro.escape(opts))})
    end
  end

  defmacro rollup_interval(ms) do
    quote do
      Module.put_attribute(__MODULE__, :rollup_interval_ms, unquote(ms))
    end
  end

  defmacro retention_interval(ms) do
    quote do
      Module.put_attribute(__MODULE__, :retention_interval_ms, unquote(ms))
    end
  end

  # --- Public helpers (called at compile time and runtime) ---

  def duration_to_seconds(:forever), do: :forever
  def duration_to_seconds({n, :seconds}), do: n
  def duration_to_seconds({n, :minutes}), do: n * 60
  def duration_to_seconds({n, :hours}), do: n * 3_600
  def duration_to_seconds({n, :days}), do: n * 86_400
  def duration_to_seconds({n, :weeks}), do: n * 604_800

  def resolution_to_seconds(:minute), do: 60
  def resolution_to_seconds(:hour), do: 3_600
  def resolution_to_seconds(:day), do: 86_400
  def resolution_to_seconds(:week), do: 604_800
  def resolution_to_seconds({n, :seconds}), do: n
  def resolution_to_seconds({n, :minutes}), do: n * 60
  def resolution_to_seconds({n, :hours}), do: n * 3_600
  def resolution_to_seconds({n, :days}), do: n * 86_400

  @doc "Build a default schema when none is provided."
  def default do
    %__MODULE__{
      raw_retention_seconds: 7 * 86_400,
      rollup_interval: :timer.minutes(5),
      retention_interval: :timer.hours(1),
      tiers: [
        %Tier{
          name: :hourly,
          resolution_seconds: 3_600,
          aggregates: [:avg, :min, :max, :count, :sum, :last],
          retention_seconds: 30 * 86_400,
          table_name: "tier_hourly"
        },
        %Tier{
          name: :daily,
          resolution_seconds: 86_400,
          aggregates: [:avg, :min, :max, :count, :sum, :last],
          retention_seconds: 365 * 86_400,
          table_name: "tier_daily"
        },
        %Tier{
          name: :monthly,
          resolution_seconds: 30 * 86_400,
          aggregates: [:avg, :min, :max, :count, :sum],
          retention_seconds: :forever,
          table_name: "tier_monthly"
        }
      ]
    }
  end
end
