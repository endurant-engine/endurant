# Overview

Endurant is a durable workflow engine for Elixir.

Workflows are defined as Elixir modules and written as code. Executions state is persistet in an event log. If a worker crashes or restarts, Endurant replays those events and resumes execution deterministically.

## Features

- Workflow logic written in Elixir
- Durable execution with retry support
- Signal-based waiting for external input
- Time-based waiting with `sleep/2`
- One-time future scheduling with `Endurant.schedule/..`
- Recurring cron scheduling with `Endurant.cron/..`
- Parallel task patterns with async primitives
- Event history for debugging and auditing
