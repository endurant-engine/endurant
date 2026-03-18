defmodule Endurant.Integration.ChildWorkflowTest do
  use Endurant.TestSupport.IntegrationCase

  alias Endurant.TestSupport.PostgresHelper

  test("child_workflow completes and parent history records child events", %{
    runtime_opts: runtime_opts
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.ChildWorkflowTest.ChildWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:child:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            %{"id" => input["id"], "kind" => "child"}
          end
        end

        defmodule Endurant.Integration.ChildWorkflowTest.ParentWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:parent:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            child =
              child_workflow(
                "child",
                Endurant.Integration.ChildWorkflowTest.ChildWorkflow,
                %{"id" => input["id"]}
              )

            %{"id" => input["id"], "child" => child}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    instance = Keyword.fetch!(runtime_opts, :instance)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.ChildWorkflowTest.ParentWorkflow,
               %{"id" => "p1"},
               instance: instance
             )

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(execution.id, 8_000, runtime_opts)

    assert result == %{
             id: "p1",
             child: %{id: "p1", kind: :child}
           }

    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)

    assert Enum.any?(events, &(&1.type == :child_execution_started))
    assert Enum.any?(events, &(&1.type == :child_execution_completed))
    refute Enum.any?(events, &(&1.type == :child_execution_failed))
    refute Enum.any?(events, &(&1.type == :child_execution_cancelled))

    child_execution_id = child_execution_id_from_started_event(events)

    assert child_metadata(Keyword.fetch!(runtime_opts, :repo), child_execution_id, runtime_opts) ==
             %{
               "child_workflow" => %{
                 "child_first_execution_id" => child_execution_id,
                 "parent_child_key" => "child",
                 "parent_close_policy" => "abandon",
                 "parent_execution_id" => execution.id
               }
             }
  end

  test("child_workflow survives child continue_as_new and parent history stays authoritative", %{
    runtime_opts: runtime_opts
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.ChildWorkflowTest.ContinuingChildWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:continuing-child:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(version, input) do
            if input["continued"] do
              final = wait_signal("final")
              %{"id" => input["id"], "version" => version, "final" => final}
            else
              _go = wait_signal("go")

              continue_as_new(
                %{"id" => input["id"], "continued" => true},
                version: "2",
                rollover_signals: true
              )
            end
          end
        end

        defmodule Endurant.Integration.ChildWorkflowTest.ParentContinuingChildWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:parent-continuing:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            child =
              child_workflow(
                "child",
                Endurant.Integration.ChildWorkflowTest.ContinuingChildWorkflow,
                %{"id" => input["id"]}
              )

            %{"id" => input["id"], "child" => child}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    instance = Keyword.fetch!(runtime_opts, :instance)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.ChildWorkflowTest.ParentContinuingChildWorkflow,
               %{"id" => "p2"},
               instance: instance
             )

    assert :waiting = wait_for_status(execution.id, :waiting, 5_000, runtime_opts)

    assert :ok =
             Endurant.signal("child-workflow:continuing-child:p2", "go", %{"ok" => true},
               instance: instance
             )

    assert :ok =
             Endurant.signal(
               "child-workflow:continuing-child:p2",
               "final",
               %{"value" => "done"},
               instance: instance
             )

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(execution.id, 12_000, runtime_opts)

    assert result == %{
             id: "p2",
             child: %{id: "p2", version: "2", final: %{value: :done}}
           }

    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)

    assert 1 == Enum.count(events, &(&1.type == :child_execution_started))
    assert 1 == Enum.count(events, &(&1.type == :child_execution_completed))

    assert Enum.any?(events, fn
             %{type: :execution_waiting, payload: %{"mode" => "child", "child_key" => "child"}} ->
               true

             %{type: :execution_waiting, payload: %{mode: :child, child_key: "child"}} ->
               true

             _ ->
               false
           end)
  end

  test("child_workflow_async and child_workflow_await honor version and unique_id options", %{
    runtime_opts: runtime_opts
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.ChildWorkflowTest.OptionChildWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:option-child:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(version, input) do
            %{"id" => input["id"], "version" => version}
          end
        end

        defmodule Endurant.Integration.ChildWorkflowTest.OptionParentWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:option-parent:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            handle =
              child_workflow_async(
                "child",
                Endurant.Integration.ChildWorkflowTest.OptionChildWorkflow,
                %{"id" => input["id"]},
                version: "7",
                unique_id: "child-workflow:custom:#{input["id"]}"
              )

            child = child_workflow_await(handle)

            %{"child" => child}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    instance = Keyword.fetch!(runtime_opts, :instance)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.ChildWorkflowTest.OptionParentWorkflow,
               %{"id" => "p3"},
               instance: instance
             )

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(execution.id, 8_000, runtime_opts)

    assert result == %{child: %{id: "p3", version: "7"}}

    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    child_execution_id = child_execution_id_from_started_event(events)

    child_execution =
      child_execution_row(Keyword.fetch!(runtime_opts, :repo), child_execution_id, runtime_opts)

    assert child_execution.version == "7"
    assert child_execution.unique_id == "child-workflow:custom:p3"
  end

  test("closed parent does not receive late child terminal events and child metadata is kept", %{
    runtime_opts: runtime_opts
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.ChildWorkflowTest.AsyncOnlyChildWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:async-only-child:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            _ = wait_signal("finish")
            %{"id" => input["id"], "done" => true}
          end
        end

        defmodule Endurant.Integration.ChildWorkflowTest.AsyncOnlyParentWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:async-only-parent:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            _handle =
              child_workflow_async(
                "child",
                Endurant.Integration.ChildWorkflowTest.AsyncOnlyChildWorkflow,
                %{"id" => input["id"]},
                close_policy: :abandon
              )

            %{"id" => input["id"], "started" => true}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    instance = Keyword.fetch!(runtime_opts, :instance)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.ChildWorkflowTest.AsyncOnlyParentWorkflow,
               %{"id" => "p4"},
               instance: instance
             )

    assert {:ok, %{status: :completed, result: %{id: "p4", started: true}}} =
             PostgresHelper.wait_for_execution!(execution.id, 8_000, runtime_opts)

    {:ok, parent_events_before} = PostgresHelper.history(execution.id, runtime_opts)
    child_execution_id = child_execution_id_from_started_event(parent_events_before)

    assert :waiting = wait_for_status(child_execution_id, :waiting, 8_000, runtime_opts)

    assert :ok =
             Endurant.signal("child-workflow:async-only-child:p4", "finish", %{"ok" => true},
               instance: instance
             )

    assert {:ok, %{status: :completed}} =
             PostgresHelper.wait_for_execution!(child_execution_id, 8_000, runtime_opts)

    {:ok, parent_events_after} = PostgresHelper.history(execution.id, runtime_opts)

    assert 1 == Enum.count(parent_events_after, &(&1.type == :child_execution_started))
    assert 0 == Enum.count(parent_events_after, &(&1.type == :child_execution_completed))

    assert child_metadata(Keyword.fetch!(runtime_opts, :repo), child_execution_id, runtime_opts) ==
             %{
               "child_workflow" => %{
                 "child_first_execution_id" => child_execution_id,
                 "parent_child_key" => "child",
                 "parent_close_policy" => "abandon",
                 "parent_execution_id" => execution.id
               }
             }
  end

  test("parent close policy request_cancel cancels open child workflows", %{
    runtime_opts: runtime_opts
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.ChildWorkflowTest.RequestCancelChildWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:request-cancel-child:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, _input) do
            _ = wait_signal("finish")
            %{"ok" => true}
          end
        end

        defmodule Endurant.Integration.ChildWorkflowTest.RequestCancelParentWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:request-cancel-parent:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            _ =
              child_workflow_async(
                "child",
                Endurant.Integration.ChildWorkflowTest.RequestCancelChildWorkflow,
                %{"id" => input["id"]},
                close_policy: :request_cancel
              )

            %{"id" => input["id"], "started" => true}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    instance = Keyword.fetch!(runtime_opts, :instance)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.ChildWorkflowTest.RequestCancelParentWorkflow,
               %{"id" => "p5"},
               instance: instance
             )

    assert {:ok, %{status: :completed, result: %{id: "p5", started: true}}} =
             PostgresHelper.wait_for_execution!(execution.id, 8_000, runtime_opts)

    {:ok, parent_events} = PostgresHelper.history(execution.id, runtime_opts)
    child_execution_id = child_execution_id_from_started_event(parent_events)

    assert :cancelled = wait_for_status(child_execution_id, :cancelled, 8_000, runtime_opts)
  end

  test("reusing the same child key returns the same child handle and terminal event once", %{
    runtime_opts: runtime_opts
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.ChildWorkflowTest.DuplicateKeyChildWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:duplicate-key-child:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            %{"id" => input["id"]}
          end
        end

        defmodule Endurant.Integration.ChildWorkflowTest.DuplicateKeyParentWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:duplicate-key-parent:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            first =
              child_workflow_async(
                "child",
                Endurant.Integration.ChildWorkflowTest.DuplicateKeyChildWorkflow,
                %{"id" => input["id"]}
              )

            second =
              child_workflow_async(
                "child",
                Endurant.Integration.ChildWorkflowTest.DuplicateKeyChildWorkflow,
                %{"id" => input["id"]}
              )

            child = child_workflow_await(first)
            child_again = child_workflow_await(second)

            %{
              "same_execution" => first.child_execution_id == second.child_execution_id,
              "same_result" => child == child_again
            }
          end
        end
      end

    Code.compile_quoted(workflow_module)

    instance = Keyword.fetch!(runtime_opts, :instance)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.ChildWorkflowTest.DuplicateKeyParentWorkflow,
               %{"id" => "p6"},
               instance: instance
             )

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(execution.id, 8_000, runtime_opts)

    assert result == %{same_execution: true, same_result: true}

    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert 1 == Enum.count(events, &(&1.type == :child_execution_started))
    assert 1 == Enum.count(events, &(&1.type == :child_execution_completed))
  end

  test("child failure is recorded once and fails the parent", %{runtime_opts: runtime_opts}) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.ChildWorkflowTest.FailingChildWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:failing-child:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, _input) do
            raise "boom"
          end
        end

        defmodule Endurant.Integration.ChildWorkflowTest.FailingParentWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:failing-parent:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            _ =
              child_workflow(
                "child",
                Endurant.Integration.ChildWorkflowTest.FailingChildWorkflow,
                %{"id" => input["id"]}
              )

            %{"ok" => true}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    instance = Keyword.fetch!(runtime_opts, :instance)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.ChildWorkflowTest.FailingParentWorkflow,
               %{"id" => "p7"},
               instance: instance
             )

    assert {:ok, %{status: :failed, result: result}} =
             PostgresHelper.wait_for_execution!(execution.id, 8_000, runtime_opts)

    assert inspect(result) =~ "child workflow"
    assert inspect(result) =~ "boom"

    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert 1 == Enum.count(events, &(&1.type == :child_execution_failed))
    assert 0 == Enum.count(events, &(&1.type == :child_execution_completed))
    assert 0 == Enum.count(events, &(&1.type == :child_execution_cancelled))
  end

  test("child cancellation is recorded once and fails the parent", %{runtime_opts: runtime_opts}) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.ChildWorkflowTest.CancellableChildWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:cancellable-child:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, _input) do
            _ = wait_signal("finish")
            %{"ok" => true}
          end
        end

        defmodule Endurant.Integration.ChildWorkflowTest.CancellableParentWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:cancellable-parent:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            _ =
              child_workflow(
                "child",
                Endurant.Integration.ChildWorkflowTest.CancellableChildWorkflow,
                %{"id" => input["id"]}
              )

            %{"ok" => true}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    instance = Keyword.fetch!(runtime_opts, :instance)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.ChildWorkflowTest.CancellableParentWorkflow,
               %{"id" => "p8"},
               instance: instance
             )

    assert :waiting = wait_for_status(execution.id, :waiting, 5_000, runtime_opts)

    {:ok, parent_events} = PostgresHelper.history(execution.id, runtime_opts)
    child_execution_id = child_execution_id_from_started_event(parent_events)

    assert :ok =
             Endurant.cancel(child_execution_id, instance: instance)

    assert {:ok, %{status: :failed, result: result}} =
             PostgresHelper.wait_for_execution!(execution.id, 8_000, runtime_opts)

    assert inspect(result) =~ "child workflow"
    assert inspect(result) =~ "was cancelled"

    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert 1 == Enum.count(events, &(&1.type == :child_execution_cancelled))
    assert 0 == Enum.count(events, &(&1.type == :child_execution_completed))
    assert 0 == Enum.count(events, &(&1.type == :child_execution_failed))
  end

  test("child metadata survives child continue_as_new on the latest child run", %{
    runtime_opts: runtime_opts
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.ChildWorkflowTest.MetadataContinuingChildWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:metadata-continuing-child:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            if input["continued"] do
              %{"id" => input["id"], "continued" => true}
            else
              continue_as_new(%{"id" => input["id"], "continued" => true})
            end
          end
        end

        defmodule Endurant.Integration.ChildWorkflowTest.MetadataContinuingParentWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:metadata-continuing-parent:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            child =
              child_workflow(
                "child",
                Endurant.Integration.ChildWorkflowTest.MetadataContinuingChildWorkflow,
                %{"id" => input["id"]}
              )

            %{"child" => child}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    instance = Keyword.fetch!(runtime_opts, :instance)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.ChildWorkflowTest.MetadataContinuingParentWorkflow,
               %{"id" => "p9"},
               instance: instance
             )

    assert {:ok, %{status: :completed}} =
             PostgresHelper.wait_for_execution!(execution.id, 8_000, runtime_opts)

    {:ok, parent_events} = PostgresHelper.history(execution.id, runtime_opts)
    first_child_execution_id = child_execution_id_from_started_event(parent_events)

    latest_child =
      latest_execution_row_by_unique_id(
        Keyword.fetch!(runtime_opts, :repo),
        "child-workflow:metadata-continuing-child:p9",
        runtime_opts
      )

    assert latest_child.metadata == %{
             "child_workflow" => %{
               "child_first_execution_id" => first_child_execution_id,
               "parent_child_key" => "child",
               "parent_close_policy" => "abandon",
               "parent_execution_id" => execution.id
             }
           }
  end

  test(
    "mark_waiting_with_child_event_owned returns already_resolved when parent history is already terminal",
    %{
      runtime_opts: runtime_opts
    }
  ) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.ChildWorkflowTest.RaceParentWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:race-parent:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input), do: input
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.Executions.insert(
               Endurant.Integration.ChildWorkflowTest.RaceParentWorkflow,
               %{"id" => "p10"},
               runtime_opts
             )

    assert [%{id: execution_id}] =
             Endurant.Executions.claim_pending(
               :orders,
               1,
               "test-worker:child-race",
               5_000,
               runtime_opts
             )

    assert execution_id == execution.id

    child_execution_id = Ecto.UUID.generate()

    :ok =
      Endurant.Events.append(
        execution.id,
        :child_execution_completed,
        %{
          child_key: "child",
          child_execution_id: child_execution_id,
          child_first_execution_id: child_execution_id,
          child_unique_id: "child-workflow:race-child:p10",
          result: %{"done" => true}
        },
        runtime_opts
      )

    assert :already_resolved =
             Endurant.Executions.mark_waiting_with_child_event_owned(
               execution.id,
               "test-worker:child-race",
               "child",
               "child-workflow:race-child:p10",
               child_execution_id,
               runtime_opts
             )

    assert %{status: :running} = Endurant.Executions.get(execution.id, runtime_opts)
  end

  test("parent waiting on child resumes after parked executor crash", %{
    runtime_opts: runtime_opts,
    engine_name: engine_name
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.ChildWorkflowTest.ParkedRecoveryChildWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:parked-recovery-child:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            _ = wait_signal("finish_child")
            %{"id" => input["id"], "done" => true}
          end
        end

        defmodule Endurant.Integration.ChildWorkflowTest.ParkedRecoveryParentWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "child-workflow:parked-recovery-parent:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            child =
              child_workflow(
                "child",
                Endurant.Integration.ChildWorkflowTest.ParkedRecoveryChildWorkflow,
                %{"id" => input["id"]}
              )

            %{"id" => input["id"], "child" => child}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    instance = Keyword.fetch!(runtime_opts, :instance)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.ChildWorkflowTest.ParkedRecoveryParentWorkflow,
               %{"id" => "p12"},
               instance: instance
             )

    child_execution_id = wait_for_child_started(execution.id, 8_000, runtime_opts)
    assert :waiting = wait_for_status(execution.id, :waiting, 8_000, runtime_opts)
    assert :waiting = wait_for_status(child_execution_id, :waiting, 8_000, runtime_opts)

    kill_parked_executor!(execution.id, engine_name)
    force_lock_expired!(execution.id, runtime_opts)
    Process.sleep(150)

    assert %{status: :waiting} = Endurant.execution(execution.id, instance: instance)

    assert :ok =
             Endurant.signal(child_execution_id, "finish_child", %{"ok" => true},
               instance: instance
             )

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(execution.id, 8_000, runtime_opts)

    assert result == %{
             id: "p12",
             child: %{id: "p12", done: true}
           }

    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert Enum.any?(events, &(&1.type == :execution_abandoned))
    assert Enum.any?(events, &(&1.type == :execution_resumed))
    assert 1 == Enum.count(events, &(&1.type == :child_execution_completed))
  end

  @spec wait_for_status(binary(), atom(), timeout(), keyword()) :: atom()
  defp wait_for_status(execution_id, status, timeout_ms, runtime_opts) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    instance = Keyword.fetch!(runtime_opts, :instance)
    do_wait_for_status(instance, execution_id, status, deadline)
  end

  @spec do_wait_for_status(term(), binary(), atom(), integer()) :: atom()
  defp do_wait_for_status(instance, execution_id, status, deadline) do
    case Endurant.execution(execution_id, instance: instance) do
      %{status: ^status} ->
        status

      _ ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("execution #{execution_id} did not reach status #{inspect(status)}")
        else
          Process.sleep(25)
          do_wait_for_status(instance, execution_id, status, deadline)
        end
    end
  end

  @spec wait_for_child_started(binary(), timeout(), keyword()) :: binary()
  defp wait_for_child_started(execution_id, timeout_ms, runtime_opts) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_child_started(execution_id, deadline, runtime_opts)
  end

  @spec do_wait_for_child_started(binary(), integer(), keyword()) :: binary()
  defp do_wait_for_child_started(execution_id, deadline, runtime_opts) do
    {:ok, events} = PostgresHelper.history(execution_id, runtime_opts)

    case Enum.find_value(events, fn
           %{type: :child_execution_started, payload: %{"child_execution_id" => child_execution_id}} ->
             child_execution_id

           %{type: :child_execution_started, payload: %{child_execution_id: child_execution_id}} ->
             child_execution_id

           _ ->
             nil
         end) do
      child_execution_id when is_binary(child_execution_id) ->
        child_execution_id

      nil ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("parent #{execution_id} did not record child_execution_started")
        else
          Process.sleep(25)
          do_wait_for_child_started(execution_id, deadline, runtime_opts)
        end
    end
  end

  @spec child_execution_id_from_started_event([map()]) :: binary()
  defp child_execution_id_from_started_event(events) do
    Enum.find_value(events, fn
      %{type: :child_execution_started, payload: %{"child_execution_id" => child_execution_id}} ->
        child_execution_id

      %{type: :child_execution_started, payload: %{child_execution_id: child_execution_id}} ->
        child_execution_id

      _ ->
        nil
    end) || flunk("missing child_execution_started event")
  end

  @spec child_metadata(module(), binary(), keyword()) :: map()
  defp child_metadata(repo, child_execution_id, runtime_opts) do
    child_execution_row(repo, child_execution_id, runtime_opts).metadata
  end

  @spec child_execution_row(module(), binary(), keyword()) :: map()
  defp child_execution_row(repo, child_execution_id, runtime_opts) do
    prefix = Keyword.get(runtime_opts, :prefix, "public")

    sql = """
    SELECT unique_id, version, metadata
    FROM #{prefix}.endurant_executions
    WHERE id = $1
    """

    case repo.query!(sql, [Ecto.UUID.dump!(child_execution_id)], log: false).rows do
      [[unique_id, version, metadata]] ->
        %{unique_id: unique_id, version: version, metadata: metadata || %{}}

      _ ->
        flunk("missing child execution row #{child_execution_id}")
    end
  end

  @spec latest_execution_row_by_unique_id(module(), String.t(), keyword()) :: map()
  defp latest_execution_row_by_unique_id(repo, unique_id, runtime_opts) do
    prefix = Keyword.get(runtime_opts, :prefix, "public")

    sql = """
    SELECT id, unique_id, version, metadata
    FROM #{prefix}.endurant_executions
    WHERE unique_id = $1
    ORDER BY inserted_at DESC, id DESC
    LIMIT 1
    """

    case repo.query!(sql, [unique_id], log: false).rows do
      [[id, row_unique_id, version, metadata]] ->
        %{
          id: Ecto.UUID.load!(id),
          unique_id: row_unique_id,
          version: version,
          metadata: metadata || %{}
        }

      _ ->
        flunk("missing execution row for unique_id #{unique_id}")
    end
  end

  @spec force_lock_expired!(binary(), keyword()) :: :ok
  defp force_lock_expired!(execution_id, runtime_opts) do
    prefix = Keyword.fetch!(runtime_opts, :prefix)

    PostgresHelper.Repo.query!(
      "UPDATE #{prefix}.endurant_executions SET locked_until = timezone('UTC', now()) - interval '1 second', updated_at = timezone('UTC', now()) WHERE id = $1",
      [to_db_id(execution_id)]
    )

    :ok
  end

  @spec to_db_id(binary()) :: binary()
  defp to_db_id(id) do
    case Ecto.UUID.dump(id) do
      {:ok, dumped} -> dumped
      :error -> id
    end
  end

  @spec kill_parked_executor!(binary(), String.t(), pos_integer()) :: :ok
  defp kill_parked_executor!(execution_id, engine_name, timeout_ms \\ 2000) do
    manager_name = Endurant.Supervisor.queue_manager_name(engine_name, :orders)
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    pid = find_parked_executor_pid!(manager_name, execution_id, deadline)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
    after
      timeout_ms -> raise "parked executor #{inspect(pid)} did not exit in #{timeout_ms}ms"
    end
  end

  @spec find_parked_executor_pid!(term(), binary(), integer()) :: pid()
  defp find_parked_executor_pid!(manager_name, execution_id, deadline_ms) do
    state = :sys.get_state(manager_name)
    match = Enum.find(state.parked, fn {_ref, info} -> info.execution_id == execution_id end)

    case match do
      {_ref, %{pid: pid}} when is_pid(pid) ->
        pid

      _ ->
        if System.monotonic_time(:millisecond) >= deadline_ms do
          raise "parked executor not found for #{execution_id}"
        else
          Process.sleep(20)
          find_parked_executor_pid!(manager_name, execution_id, deadline_ms)
        end
    end
  end
end
