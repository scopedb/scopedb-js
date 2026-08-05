# ScopeDB JavaScript SDK examples

Every example is self-contained and imports the public `scopedb` package entry.
Start with a quickstart, then move to a production pattern only when its delivery
tradeoffs match the workload.

The commands below are for a source checkout of this repository. The published
npm package includes the TypeScript examples as reference source.

## Read-only quickstarts

These can run against a reachable ScopeDB server without modifying data.

| Example | Shows | Run |
| --- | --- | --- |
| [`statement.ts`](statement.ts) | Query execution and JSON-safe integer results | `pnpm run example:statement` |
| [`catalog.ts`](catalog.ts) | Complete REST catalog pagination and table metadata | `pnpm run example:catalog` |

## Before running a write example

Write examples intentionally refuse to start without `SCOPEDB_TABLE`. Use a
disposable, non-production table and do not point them at a production endpoint
unless the writes are intentional.

The append, stream, telemetry, Serverless, and transform examples can share this
superset schema:

```sql
CREATE TABLE public.sdk_example_events (
  id int,
  event_id string,
  occurred_at timestamp,
  name string,
  path string,
  status int,
  attributes object
);
```

Configuration is read from:

- `SCOPEDB_ENDPOINT` (defaults to `http://127.0.0.1:6543`)
- `SCOPEDB_TOKEN`
- `SCOPEDB_DATABASE` (defaults to `scopedb`)
- `SCOPEDB_SCHEMA` (defaults to `public`)
- `SCOPEDB_TABLE` (required for writes)

Set the disposable target once in the current shell before using the write
commands below:

```sh
export SCOPEDB_TABLE=sdk_example_events
```

```powershell
$env:SCOPEDB_TABLE = "sdk_example_events"
```

## Append quickstarts

Use these to learn the two basic write paths before copying a tuned pattern.

| Example | Choose it when | Run |
| --- | --- | --- |
| [`append.ts`](append.ts) | The caller already owns one exact NDJSON payload | `pnpm run example:append` |
| [`append-stream.ts`](append-stream.ts) | The SDK should asynchronously batch object rows | `pnpm run example:append-stream` |

`append.ts` sends exactly one request. `append-stream.ts` uses the default strict
policy without tuning knobs: `sendAll()` waits only for local admission and
does not confirm a remote commit, while a successful `shutdown()` confirms its
accepted prefix committed.

## Runnable production patterns

These are complete demos but make workload-specific tradeoffs.

| Example | Workload | Important boundary | Run |
| --- | --- | --- | --- |
| [`patterns/bulk-import.ts`](patterns/bulk-import.ts) | Files and backfills | Bounded concurrency, but no durable resume or whole-job rollback | `pnpm run example:append-bulk` |
| [`patterns/telemetry.ts`](patterns/telemetry.ts) | Long-running logs and events | Best effort; drops and remote loss are observable but payloads are not retained | `pnpm run example:append-telemetry` |
| [`ingest-transform.ts`](ingest-transform.ts) | SQL transformation before insert | Uses the transform-oriented ingest API instead of table append | `pnpm run example:ingest-transform` |

## Integration templates

Templates are type-checked but are not directly executable. Copy the entire
file so its lifecycle or durability contract stays with the implementation.

| Template | Requires | Delivery model |
| --- | --- | --- |
| [`templates/serverless.ts`](templates/serverless.ts) | Node 20 Fetch `Request`/`Response` and a `waitUntil()` hook | Module-level best-effort stream with lifecycle-backed barriers |
| [`templates/audit-outbox.ts`](templates/audit-outbox.ts) | An application-owned transactional outbox | One immutable durable attempt per HTTP request, with crash-safe reconciliation |

For audit delivery, the outbox must persist `READY -> ATTEMPTING` before the
network call. Crash recovery routes an incomplete `ATTEMPTING` record to
reconciliation and never appends that same attempt again. Stable event IDs do
not make the destination automatically idempotent.

## Delivery contract

- `send()`, `sendAll()`, and a `true` result from `trySend()` mean only that
  local memory admitted the row; they do not mean the row committed remotely.
- A successful strict barrier confirms its accepted prefix committed.
- A continue-mode barrier is settlement; always inspect its
  `AppendDeliveryReport`.
- The stream retries only the exact temporary HTTP batch explicitly marked
  `rejected`; never infer that an entire stream or source is safe to rerun.
- A timeout or transport failure is `unknown`; do not blindly replay it.
- `shutdown()` permanently closes the stream after settling the accepted
  prefix. It is not an abort or rollback.
- Stop and join producer tasks before shutdown.
- Use `sendAll()` rather than creating one Promise per row with `Promise.all()`.
- An in-memory stream is not a durable queue; audit delivery needs an outbox.

## Developing the examples

List or run executable examples through the central runner:

```sh
pnpm run example:list
pnpm run example -- statement
```

Check every example against the built package entry, or emit their JavaScript:

```sh
pnpm run check:examples
pnpm run build:examples
```
