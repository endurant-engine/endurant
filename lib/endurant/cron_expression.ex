defmodule Endurant.CronExpression do
  @moduledoc false

  @type t :: %__MODULE__{
          seconds: :any | MapSet.t(non_neg_integer()),
          minutes: :any | MapSet.t(non_neg_integer()),
          hours: :any | MapSet.t(non_neg_integer()),
          day_of_month: :any | MapSet.t(pos_integer()),
          month: :any | MapSet.t(pos_integer()),
          day_of_week: :any | MapSet.t(non_neg_integer())
        }

  @enforce_keys [:seconds, :minutes, :hours, :day_of_month, :month, :day_of_week]
  defstruct [:seconds, :minutes, :hours, :day_of_month, :month, :day_of_week]

  @max_steps 5_000_000

  @spec parse(String.t()) :: {:ok, t()} | {:error, :invalid_cron_expression}
  def parse(expr) when is_binary(expr) do
    fields =
      expr
      |> String.trim()
      |> String.split(~r/\s+/, trim: true)

    with {:ok, second, minute, hour, day_of_month, month, day_of_week} <-
           normalize_fields(fields),
         {:ok, seconds} <- parse_field(second, 0, 59, :seconds),
         {:ok, minutes} <- parse_field(minute, 0, 59, :minutes),
         {:ok, hours} <- parse_field(hour, 0, 23, :hours),
         {:ok, day_of_month_values} <- parse_field(day_of_month, 1, 31, :day_of_month),
         {:ok, month_values} <- parse_field(month, 1, 12, :month),
         {:ok, day_of_week_values} <- parse_field(day_of_week, 0, 7, :day_of_week) do
      {:ok,
       %__MODULE__{
         seconds: seconds,
         minutes: minutes,
         hours: hours,
         day_of_month: day_of_month_values,
         month: month_values,
         day_of_week: normalize_day_of_week(day_of_week_values)
       }}
    else
      _ -> {:error, :invalid_cron_expression}
    end
  end

  def parse(_expr), do: {:error, :invalid_cron_expression}

  @spec next_after(t(), DateTime.t(), String.t()) :: {:ok, DateTime.t()} | {:error, term()}
  def next_after(%__MODULE__{} = expr, %DateTime{} = after_utc, timezone)
      when is_binary(timezone) do
    with {:ok, local} <- DateTime.shift_zone(after_utc, timezone) do
      candidate =
        local
        |> DateTime.truncate(:second)
        |> DateTime.add(1, :second)

      find_next(expr, candidate, @max_steps)
    end
  end

  def next_after(%__MODULE__{}, %DateTime{}, _timezone), do: {:error, :invalid_timezone}

  @spec normalize_fields([String.t()]) ::
          {:ok, String.t(), String.t(), String.t(), String.t(), String.t(), String.t()}
          | :error
  defp normalize_fields([minute, hour, day_of_month, month, day_of_week]) do
    {:ok, "0", minute, hour, day_of_month, month, day_of_week}
  end

  defp normalize_fields([second, minute, hour, day_of_month, month, day_of_week]) do
    {:ok, second, minute, hour, day_of_month, month, day_of_week}
  end

  defp normalize_fields(_fields), do: :error

  @spec parse_field(String.t(), integer(), integer(), atom()) ::
          {:ok, :any | MapSet.t(integer())} | :error
  defp parse_field("*", _min, _max, _kind), do: {:ok, :any}

  defp parse_field(value, min, max, kind) do
    value
    |> String.split(",", trim: true)
    |> Enum.reduce_while({:ok, MapSet.new()}, fn token, {:ok, set} ->
      case parse_token(token, min, max, kind) do
        {:ok, :any} ->
          {:halt, {:ok, :any}}

        {:ok, values} ->
          {:cont, {:ok, Enum.reduce(values, set, &MapSet.put(&2, &1))}}

        :error ->
          {:halt, :error}
      end
    end)
  end

  @spec parse_token(String.t(), integer(), integer(), atom()) ::
          {:ok, :any | [integer()]} | :error
  defp parse_token("*", _min, _max, _kind), do: {:ok, :any}

  defp parse_token("*/" <> step, min, max, kind) do
    with {step_int, ""} when step_int > 0 <- Integer.parse(step) do
      {:ok, Enum.to_list(min..max//step_int) |> Enum.map(&normalize_value(&1, kind))}
    else
      _ -> :error
    end
  end

  defp parse_token(token, min, max, kind) do
    with {value, ""} <- Integer.parse(token),
         true <- value in min..max do
      {:ok, [normalize_value(value, kind)]}
    else
      _ -> :error
    end
  end

  @spec normalize_value(integer(), atom()) :: integer()
  defp normalize_value(7, :day_of_week), do: 0
  defp normalize_value(value, _kind), do: value

  @spec normalize_day_of_week(:any | MapSet.t(integer())) :: :any | MapSet.t(integer())
  defp normalize_day_of_week(:any), do: :any
  defp normalize_day_of_week(values), do: values

  @spec find_next(t(), DateTime.t(), non_neg_integer()) ::
          {:ok, DateTime.t()} | {:error, :no_next_time}
  defp find_next(_expr, _candidate, 0), do: {:error, :no_next_time}

  defp find_next(expr, candidate, remaining) do
    if matches?(expr, candidate) do
      DateTime.shift_zone(candidate, "Etc/UTC")
    else
      find_next(expr, DateTime.add(candidate, 1, :second), remaining - 1)
    end
  end

  @spec matches?(t(), DateTime.t()) :: boolean()
  defp matches?(%__MODULE__{} = expr, %DateTime{} = dt) do
    month_match = matches_field?(expr.month, dt.month)
    hour_match = matches_field?(expr.hours, dt.hour)
    minute_match = matches_field?(expr.minutes, dt.minute)
    second_match = matches_field?(expr.seconds, dt.second)

    month_match and hour_match and minute_match and second_match and day_matches?(expr, dt)
  end

  @spec day_matches?(t(), DateTime.t()) :: boolean()
  defp day_matches?(%__MODULE__{} = expr, %DateTime{} = dt) do
    dom_match = matches_field?(expr.day_of_month, dt.day)
    dow_match = matches_field?(expr.day_of_week, day_of_week(dt))
    dom_restricted? = expr.day_of_month != :any
    dow_restricted? = expr.day_of_week != :any

    cond do
      dom_restricted? and dow_restricted? -> dom_match or dow_match
      true -> dom_match and dow_match
    end
  end

  @spec day_of_week(DateTime.t()) :: non_neg_integer()
  defp day_of_week(%DateTime{} = dt) do
    dt
    |> DateTime.to_date()
    |> Date.day_of_week()
    |> rem(7)
  end

  @spec matches_field?(:any | MapSet.t(integer()), integer()) :: boolean()
  defp matches_field?(:any, _value), do: true
  defp matches_field?(set, value), do: MapSet.member?(set, value)
end
