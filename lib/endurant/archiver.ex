defmodule Endurant.Archiver do
  @moduledoc false

  @type archive_batch_item :: %{
          required(:execution) => map(),
          required(:events) => [map()]
        }

  @callback init(endurant_migration_version :: non_neg_integer(), opts :: keyword()) ::
              :ok | {:error, term()}

  @callback archive(
              batch :: [archive_batch_item()],
              endurant_migration_version :: non_neg_integer(),
              opts :: keyword()
            ) ::
              :ok | {:error, term()}
end
