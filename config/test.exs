import Config

config :endurant, :postgres,
  username: System.get_env("ENDURANT_PGUSER", "postgres"),
  password: System.get_env("ENDURANT_PGPASSWORD", "postgres"),
  hostname: System.get_env("ENDURANT_PGHOST", "localhost"),
  database: System.get_env("ENDURANT_PGDATABASE", "endurant_test"),
  pool_size: String.to_integer(System.get_env("ENDURANT_PGPOOL", "10"))
