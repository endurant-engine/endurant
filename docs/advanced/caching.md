# Caching

Caching is how Endurant handles waiting executions at runtime.

When a workflow calls `wait_signal/1` or `sleep/2`, the execution transitions to
waiting. At that point, the queue manager decides whether to:

- cache the executor process in memory for faster resume, or
- release the execution so it can be claimed later.

## Why It Exists

Caching gives a tradeoff between latency and memory usage:

- more caching: faster resume, higher memory/process usage
- less caching: lower memory usage, more claim/restart work on resume

## Options

Configured per queue in `Endurant.start_link/1`:

- `cached_limit` - max cached executions kept in memory
- `cached_ttl_ms` - max time a cached executor stays alive before it is released back to persisted waiting state
