defmodule Endurant.TestSupport.WorkflowHelpers do
  @moduledoc false

  defmodule Accounts do
    @spec fetch_user(binary()) :: %{id: binary(), premium: boolean()}
    def fetch_user(user_id), do: %{id: user_id, premium: true}
  end

  defmodule Mailer do
    @spec send_priority(binary()) :: %{channel: :priority, user_id: binary()}
    def send_priority(user_id), do: %{channel: :priority, user_id: user_id}

    @spec send_regular(binary()) :: %{channel: :regular, user_id: binary()}
    def send_regular(user_id), do: %{channel: :regular, user_id: user_id}
  end

  defmodule Orders do
    @spec process_item(binary()) :: %{item: binary(), status: :ok}
    def process_item(item), do: %{item: item, status: :ok}

    @spec finalize(binary(), map(), list(map())) :: map()
    def finalize(order_id, approval, processed) do
      %{order_id: order_id, approval: approval, processed: processed}
    end
  end

  defmodule RetryGate do
    use Agent

    @spec start_link(keyword()) :: Agent.on_start()
    def start_link(opts \\ []) do
      Agent.start_link(fn -> %{} end, opts)
    end

    @spec reset() :: :ok
    def reset do
      Agent.update(__MODULE__, fn _ -> %{} end)
    end

    @spec issue(binary()) :: %{status: :issued, order_id: binary()}
    def issue(order_id) do
      outcome =
        Agent.get_and_update(__MODULE__, fn state ->
          attempts = Map.get(state, order_id, 0) + 1
          next_state = Map.put(state, order_id, attempts)

          if attempts == 1 do
            {:fail, next_state}
          else
            {{:ok, %{status: :issued, order_id: order_id}}, next_state}
          end
        end)

      case outcome do
        :fail ->
          raise RuntimeError, "transient invoice error"

        {:ok, result} ->
          result
      end
    end
  end
end
