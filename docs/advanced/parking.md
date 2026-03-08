# Parking

Parking is how Endurant handles waiting executions at runtime.

When a workflow calls `wait_signal/1` or `sleep/2`, the execution transitions to
waiting. At that point, the queue manager decides whether to:

- park the executor process in memory for faster resume, or
- release the execution so it can be claimed later.

## Why It Exists

Parking gives a tradeoff between latency and memory usage:

- more parking: faster resume, higher memory/process usage
- less parking: lower memory usage, more claim/restart work on resume

## Options

Configured per queue in `Endurant.start_link/1`:

- `parked_limit` - max parked executions kept in memory

