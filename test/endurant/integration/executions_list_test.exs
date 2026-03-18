defmodule Endurant.Integration.ExecutionsListTest do
  use Endurant.TestSupport.IntegrationCase

  test "filters by status, queue, workflow, unique_id and execution ids", %{
    engine_name: engine_name,
    runtime_opts: runtime_opts
  } do
    base =
      NaiveDateTime.utc_now()
      |> NaiveDateTime.add(-120, :second)
      |> NaiveDateTime.truncate(:microsecond)

    id_a =
      insert_execution!(runtime_opts, %{
        unique_id: "list-a",
        queue: "manual_orders",
        workflow_name: "List.WorkflowA",
        status: :pending,
        inserted_at: NaiveDateTime.add(base, 1, :second)
      })

    id_b =
      insert_execution!(runtime_opts, %{
        unique_id: "list-b",
        queue: "manual_orders",
        workflow_name: "List.WorkflowA",
        status: :waiting,
        inserted_at: NaiveDateTime.add(base, 2, :second)
      })

    id_c =
      insert_execution!(runtime_opts, %{
        unique_id: "list-c",
        queue: "manual_payments",
        workflow_name: "List.WorkflowA",
        status: :completed,
        inserted_at: NaiveDateTime.add(base, 3, :second),
        completed_at: NaiveDateTime.add(base, 4, :second)
      })

    id_d =
      insert_execution!(runtime_opts, %{
        unique_id: "list-d",
        queue: "manual_orders",
        workflow_name: "List.WorkflowB",
        status: :failed,
        inserted_at: NaiveDateTime.add(base, 5, :second),
        completed_at: NaiveDateTime.add(base, 6, :second)
      })

    rows =
      Endurant.executions(
        status: [:pending, :waiting],
        queue: "manual_orders",
        workflow: "List.WorkflowA",
        order: :asc,
        limit: 10,
        instance: engine_name
      )

    assert Enum.map(rows, & &1.id) == [id_a, id_b]
    assert Enum.map(rows, & &1.status) == [:pending, :waiting]

    assert [%{id: ^id_c, unique_id: "list-c"}] =
             Endurant.executions(unique_id: "list-c", limit: 5, instance: engine_name)

    id_rows =
      Endurant.executions(execution_ids: [id_d, id_a], order: :asc, limit: 10, instance: engine_name)

    assert Enum.map(id_rows, & &1.id) == [id_a, id_d]
  end

  test "supports open/terminal and timestamp range filters", %{
    engine_name: engine_name,
    runtime_opts: runtime_opts
  } do
    base =
      NaiveDateTime.utc_now()
      |> NaiveDateTime.add(-120, :second)
      |> NaiveDateTime.truncate(:microsecond)

    id_open_a =
      insert_execution!(runtime_opts, %{
        unique_id: "open-a",
        queue: "manual_range",
        workflow_name: "List.WorkflowRange",
        status: :pending,
        inserted_at: NaiveDateTime.add(base, 1, :second)
      })

    id_open_b =
      insert_execution!(runtime_opts, %{
        unique_id: "open-b",
        queue: "manual_range",
        workflow_name: "List.WorkflowRange",
        status: :waiting,
        inserted_at: NaiveDateTime.add(base, 2, :second)
      })

    id_terminal_a =
      insert_execution!(runtime_opts, %{
        unique_id: "term-a",
        queue: "manual_range",
        workflow_name: "List.WorkflowRange",
        status: :failed,
        inserted_at: NaiveDateTime.add(base, 3, :second),
        completed_at: NaiveDateTime.add(base, 3, :second)
      })

    id_terminal_b =
      insert_execution!(runtime_opts, %{
        unique_id: "term-b",
        queue: "manual_range",
        workflow_name: "List.WorkflowRange",
        status: :completed,
        inserted_at: NaiveDateTime.add(base, 4, :second),
        completed_at: NaiveDateTime.add(base, 4, :second)
      })

    open_rows = Endurant.executions(open: true, order: :asc, limit: 10, instance: engine_name)
    assert Enum.map(open_rows, & &1.id) == [id_open_a, id_open_b]

    terminal_rows = Endurant.executions(terminal: true, order: :asc, limit: 10, instance: engine_name)
    assert Enum.map(terminal_rows, & &1.id) == [id_terminal_a, id_terminal_b]

    ranged_rows =
      Endurant.executions(
        inserted_after: NaiveDateTime.add(base, 2, :second),
        inserted_before: NaiveDateTime.add(base, 4, :second),
        order: :asc,
        limit: 10,
        instance: engine_name
      )

    assert Enum.map(ranged_rows, & &1.id) == [id_open_b, id_terminal_a]
  end

  test "supports keyset cursor pagination", %{
    engine_name: engine_name,
    runtime_opts: runtime_opts
  } do
    base =
      NaiveDateTime.utc_now()
      |> NaiveDateTime.add(-300, :second)
      |> NaiveDateTime.truncate(:microsecond)

    _ =
      Enum.map(1..5, fn idx ->
        insert_execution!(runtime_opts, %{
          unique_id: "cursor-#{idx}",
          queue: "manual_cursor",
          workflow_name: "List.WorkflowCursor",
          status: :pending,
          inserted_at: NaiveDateTime.add(base, idx, :second)
        })
      end)

    all_rows = Endurant.executions(queue: "manual_cursor", order: :desc, limit: 10, instance: engine_name)
    all_ids = Enum.map(all_rows, & &1.id)

    page_one = Endurant.executions(queue: "manual_cursor", order: :desc, limit: 2, instance: engine_name)
    page_one_ids = Enum.map(page_one, & &1.id)
    assert page_one_ids == Enum.take(all_ids, 2)

    cursor_row = List.last(page_one)

    page_two =
      Endurant.executions(
        queue: "manual_cursor",
        order: :desc,
        limit: 2,
        cursor: %{inserted_at: cursor_row.inserted_at, id: cursor_row.id},
        instance: engine_name
      )

    page_two_ids = Enum.map(page_two, & &1.id)
    assert page_two_ids == all_ids |> Enum.drop(2) |> Enum.take(2)
    assert MapSet.disjoint?(MapSet.new(page_one_ids), MapSet.new(page_two_ids))
  end

  @spec insert_execution!(keyword(), map()) :: binary()
  defp insert_execution!(runtime_opts, attrs) do
    repo = Keyword.fetch!(runtime_opts, :repo)
    prefix = Keyword.fetch!(runtime_opts, :prefix)
    execution_id = Map.get(attrs, :id, Ecto.UUID.generate())
    status = normalize_status!(Map.get(attrs, :status, :pending))
    inserted_at = normalize_time!(Map.get(attrs, :inserted_at, NaiveDateTime.utc_now()))
    updated_at = normalize_time!(Map.get(attrs, :updated_at, inserted_at))
    waiting_until = normalize_optional_time(Map.get(attrs, :waiting_until))
    locked_by = Map.get(attrs, :locked_by)
    locked_until = normalize_optional_time(Map.get(attrs, :locked_until))
    completed_at = normalize_optional_time(Map.get(attrs, :completed_at))
    unique_id = Map.get(attrs, :unique_id, "list:#{execution_id}")
    queue = Map.get(attrs, :queue, "manual")
    workflow_name = Map.get(attrs, :workflow_name, "List.Workflow")
    version = Map.get(attrs, :version, "1")
    input = Map.get(attrs, :input, %{})

    sql = """
    INSERT INTO #{prefix}.endurant_executions (
      id, unique_id, queue, workflow_name, version, input, status,
      waiting_until, locked_by, locked_until, completed_at, inserted_at, updated_at
    )
    VALUES (
      $1, $2, $3, $4, $5, $6, $7::#{prefix}.endurant_execution_status,
      $8, $9, $10, $11, $12, $13
    )
    """

    repo.query!(
      sql,
      [
        to_db_id(execution_id),
        unique_id,
        queue,
        workflow_name,
        version,
        input,
        status,
        waiting_until,
        locked_by,
        locked_until,
        completed_at,
        inserted_at,
        updated_at
      ],
      log: false
    )

    execution_id
  end

  @spec normalize_status!(atom() | String.t()) :: String.t()
  defp normalize_status!(status) when is_atom(status), do: Atom.to_string(status)
  defp normalize_status!(status) when is_binary(status), do: status

  @spec normalize_time!(NaiveDateTime.t() | DateTime.t()) :: NaiveDateTime.t()
  defp normalize_time!(%NaiveDateTime{} = value), do: value
  defp normalize_time!(%DateTime{} = value), do: DateTime.to_naive(value)

  @spec normalize_optional_time(nil | NaiveDateTime.t() | DateTime.t()) :: nil | NaiveDateTime.t()
  defp normalize_optional_time(nil), do: nil
  defp normalize_optional_time(value), do: normalize_time!(value)

  @spec to_db_id(binary()) :: binary()
  defp to_db_id(id) do
    case Ecto.UUID.dump(id) do
      {:ok, dumped} -> dumped
      :error -> id
    end
  end
end
