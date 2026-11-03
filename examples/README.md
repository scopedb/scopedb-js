# ScopeDB JavaScript SDK examples

Start with the [quickstart](../README.md#run-a-query), then choose a complete program or a template for your workload. See the [usage guide](../docs/usage.md) for delivery, encoding, and error handling, and [Upgrade from 0.3 to 0.4](../docs/migration-0.4.md) when upgrading.

The SDK is designed for trusted server-side runtimes. Never expose `SCOPEDB_API_KEY` in browser bundles or Next.js Client Components.

## Read-only quickstarts

To run these programs from a checkout, install dependencies with `pnpm install --frozen-lockfile` and set the connection variables below. The commands build and run the selected program. These two examples read from a reachable ScopeDB service without modifying data:

| Example                        | Shows                                                   | Command                      |
| ------------------------------ | ------------------------------------------------------- | ---------------------------- |
| [`statement.ts`](statement.ts) | Async statement lifecycle and JSON-safe integer results | `pnpm run example:statement` |
| [`catalog.ts`](catalog.ts)     | Declarative catalog snapshots and table definitions     | `pnpm run example:catalog`   |

## Write examples

All runnable examples read connection settings from environment variables. Write examples also require `SCOPEDB_TABLE`:

| Variable           | Default                 | Purpose                                  |
| ------------------ | ----------------------- | ---------------------------------------- |
| `SCOPEDB_ENDPOINT` | `http://127.0.0.1:6543` | ScopeDB server endpoint                  |
| `SCOPEDB_API_KEY`  | None                    | Bearer API key for server authentication |
| `SCOPEDB_DATABASE` | `scopedb`               | Target database name                     |
| `SCOPEDB_SCHEMA`   | `public`                | Target schema name                       |
| `SCOPEDB_TABLE`    | Required for writes     | Target table name                        |

Create a disposable table before running the write examples:

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

```sh
export SCOPEDB_TABLE=sdk_example_events
```

| Example                                              | Use case                                                                | Command                             |
| ---------------------------------------------------- | ----------------------------------------------------------------------- | ----------------------------------- |
| [`append.ts`](append.ts)                             | Appending a pre-encoded NDJSON string with `table.append()`             | `pnpm run example:append`           |
| [`append-stream.ts`](append-stream.ts)               | Batching JavaScript objects with `table.appendStream()`                 | `pnpm run example:append-stream`    |
| [`patterns/bulk-import.ts`](patterns/bulk-import.ts) | Streaming a large iterable with bounded backpressure (`sendAll()`)      | `pnpm run example:append-bulk`      |
| [`patterns/telemetry.ts`](patterns/telemetry.ts)     | Non-blocking telemetry sink (`failurePolicy: "continue"`, `trySend()`)  | `pnpm run example:append-telemetry` |
| [`import-ndjson.ts`](import-ndjson.ts)               | Importing an NDJSON file (`SCOPEDB_INPUT_FILE`) without parsing numbers | `pnpm run example:import-ndjson`    |

## Integration templates

Copy a template into your application and supply the host integration described below. An outbox needs durable storage and a checkpoint implementation; a serverless handler needs the runtime's lifecycle hook.

| Template                                                 | Target integration                                                 | Purpose                                                             |
| -------------------------------------------------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------- |
| [`templates/retry-append.ts`](templates/retry-append.ts) | Direct `table.append()` with an `AbortSignal` deadline             | Bounded retry loop that replays only confirmed temporary rejections |
| [`templates/audit-outbox.ts`](templates/audit-outbox.ts) | Durable source implementing `readUnconfirmed()` and `checkpoint()` | At-least-once outbox delivery that checkpoints after `shutdown()`   |
| [`templates/serverless.ts`](templates/serverless.ts)     | Fetch handlers providing a `waitUntil()` lifecycle hook            | Warm-invocation telemetry stream that flushes inside `waitUntil()`  |

## Framework and runtime templates

| Runtime                 | Template                                                                                     | Notes                                                                                                     |
| ----------------------- | -------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Next.js                 | [`frameworks/nextjs-route-handler/route.ts`](frameworks/nextjs-route-handler/route.ts)       | App Router Route Handler with `server-only`, bearer write authentication (`APP_WRITE_TOKEN`), and limits  |
| Cloudflare Workers      | [`frameworks/cloudflare-worker/worker.ts`](frameworks/cloudflare-worker/worker.ts)           | Standard Web Fetch APIs without `nodejs_compat`; store `SCOPEDB_API_KEY` and `APP_WRITE_TOKEN` as secrets |
| Cloudflare Tail Workers | [`frameworks/cloudflare-worker/tail-worker.ts`](frameworks/cloudflare-worker/tail-worker.ts) | Appends Worker invocation summaries, console logs, and uncaught exceptions without `nodejs_compat`        |
| Bun                     | Any runnable ESM example                                                                     | Install with `bun add scopedb`; uses standard Web Fetch and CompressionStream APIs                        |

For SDK development and example validation, see [Contributing](../CONTRIBUTING.md).
