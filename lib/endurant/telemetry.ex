defmodule Endurant.Telemetry do
  @moduledoc false

  @spec emit([atom()], map(), map()) :: :ok
  def emit(event, measurements, metadata \\ %{})
      when is_list(event) and is_map(measurements) and is_map(metadata) do
    :telemetry.execute(
      [:endurant | event],
      normalize_measurements(measurements),
      normalize_metadata(metadata)
    )
  end

  @spec monotonic_time() :: integer()
  def monotonic_time, do: System.monotonic_time()

  @spec duration_ms(integer()) :: non_neg_integer()
  def duration_ms(start_native) when is_integer(start_native) do
    System.monotonic_time()
    |> Kernel.-(start_native)
    |> System.convert_time_unit(:native, :millisecond)
    |> max(0)
  end

  @spec datetime_diff_ms(DateTime.t() | NaiveDateTime.t(), DateTime.t() | NaiveDateTime.t()) ::
          non_neg_integer()
  def datetime_diff_ms(%DateTime{} = started_at, %DateTime{} = ended_at) do
    DateTime.diff(ended_at, started_at, :millisecond)
    |> max(0)
  end

  def datetime_diff_ms(%NaiveDateTime{} = started_at, %NaiveDateTime{} = ended_at) do
    NaiveDateTime.diff(ended_at, started_at, :millisecond)
    |> max(0)
  end

  def datetime_diff_ms(%DateTime{} = started_at, %NaiveDateTime{} = ended_at) do
    datetime_diff_ms(DateTime.to_naive(started_at), ended_at)
  end

  def datetime_diff_ms(%NaiveDateTime{} = started_at, %DateTime{} = ended_at) do
    datetime_diff_ms(started_at, DateTime.to_naive(ended_at))
  end

  @spec error_kind(term()) :: String.t()
  def error_kind(%{kind: kind}) when kind in [:exception, :throw, :exit, :error, :heartbeat_failed] do
    normalize_label(kind)
  end

  def error_kind(%{"kind" => kind})
      when kind in [:exception, :throw, :exit, :error, :heartbeat_failed] do
    normalize_label(kind)
  end

  def error_kind(%{module: _module}), do: "exception"
  def error_kind(%{"module" => _module}), do: "exception"

  def error_kind(%{reason: reason}) when is_binary(reason) do
    if String.starts_with?(reason, "{:heartbeat_failed") do
      "heartbeat_failed"
    else
      "other"
    end
  end

  def error_kind(%{"reason" => reason}) when is_binary(reason) do
    if String.starts_with?(reason, "{:heartbeat_failed") do
      "heartbeat_failed"
    else
      "other"
    end
  end

  def error_kind(_), do: "other"

  @spec normalize_metadata(map()) :: map()
  def normalize_metadata(metadata) when is_map(metadata) do
    Enum.reduce(metadata, %{}, fn
      {_key, nil}, acc ->
        acc

      {key, value}, acc ->
        Map.put(acc, key, normalize_metadata_value(value))
    end)
  end

  @spec normalize_measurements(map()) :: map()
  def normalize_measurements(measurements) when is_map(measurements) do
    Enum.reduce(measurements, %{}, fn
      {_key, nil}, acc -> acc
      {key, value}, acc -> Map.put(acc, key, value)
    end)
  end

  @spec normalize_label(term()) :: String.t()
  def normalize_label(value) do
    value
    |> normalize_metadata_value()
    |> case do
      boolean when is_boolean(boolean) -> Atom.to_string(boolean)
      normalized -> normalized
    end
  end

  @spec normalize_metadata_value(term()) :: String.t() | boolean()
  defp normalize_metadata_value(value) when is_boolean(value), do: value

  defp normalize_metadata_value(value) when is_atom(value) do
    value
    |> Atom.to_string()
    |> String.downcase()
  end

  defp normalize_metadata_value(value) when is_binary(value), do: String.downcase(value)
  defp normalize_metadata_value(value) when is_integer(value), do: Integer.to_string(value)
  defp normalize_metadata_value(value) when is_float(value), do: :erlang.float_to_binary(value)
  defp normalize_metadata_value(value), do: value |> inspect() |> String.downcase()
end
