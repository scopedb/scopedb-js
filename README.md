# ScopeDB JavaScript SDK

This package provides a TypeScript-first ScopeDB client for trusted server-side
JavaScript runtimes. It is ESM-only and has no runtime dependencies.

## Runtime support

| Environment | Recommended usage |
| --- | --- |
| Node.js 20+ | Native ESM; Node 20 is a compatibility floor, so use a maintained Node LTS release in production |
| Next.js | Route Handlers, Server Actions, and other `server-only` modules; prefer the Node runtime |
| Bun | Use the same ESM API and `bun add scopedb`; no Bun-specific adapter |
| Cloudflare Workers | Use secret bindings and Web APIs; `nodejs_compat` is not required |
| CommonJS | Load the ESM package with dynamic `import()` |

Do not use this SDK from browser code, Next.js Client Components, or other
untrusted clients. A ScopeDB API key grants server access and must not be
included in a browser bundle. The [Next.js](examples/frameworks/nextjs-route-handler/route.ts)
and [Cloudflare Worker](examples/frameworks/cloudflare-worker/worker.ts)
templates show the intended boundary.

## ScopeQL documentation

This SDK executes ScopeQL statements; the language is documented separately:

- [Quickstart](https://docs.scopedb.io/guides/quickstart)
- [Query guide](https://docs.scopedb.io/guides/query-events)
- [Language reference](https://docs.scopedb.io/reference/)

## Installation

```sh
pnpm add scopedb
# or: npm install scopedb
# or: bun add scopedb
```

## Create a Client

```ts
import { Client } from "scopedb";

const client = new Client(process.env.SCOPEDB_ENDPOINT!, {
  apiKey: process.env.SCOPEDB_API_KEY!,
});
```

The SDK compresses JSON request bodies and table append requests with gzip by
default using the Web Compression API. Append limits are based on the
uncompressed NDJSON body.

## Run a Statement

```ts
import { Client } from "scopedb";

const client = new Client(process.env.SCOPEDB_ENDPOINT!, {
  apiKey: process.env.SCOPEDB_API_KEY!,
});

const result = await client.query("SELECT 1 AS ready");
console.log(result.toObjects());
```

For a detached or long-running statement, keep its handle and choose between a
local snapshot, one remote status request, or waiting for the result:

```ts
const handle = await client.statement("SELECT 1 AS ready").submit();

// Synchronous and local: the snapshot updated by submit(), status(), or wait().
console.log(handle.lastStatus()?.status);

// Asynchronous: requests the latest remote status while the statement is active.
const latest = await handle.status();
console.log(latest.status);

// Polls until the statement terminates and returns its result set.
const result = await handle.wait();
```

`status()` returns the cached snapshot without another request once the handle
has reached a terminal state. Use `client.statementHandle(id)` to resume this
lifecycle from a previously stored statement ID. `wait()`, `query()`, and
`execute()` accept `WaitOptions` when polling delays or cancellation need to be
configured.

## Integer Representation

`int` and `uint` cells default to JS `bigint` to preserve full I64 precision.
This is the safe default but is **not** directly JSON-serializable —
`JSON.stringify(rowWithBigInt)` throws `TypeError: Do not know how to serialize
a BigInt`.

`toValues()`, `toObjects()`, and `first()` accept an optional
`{ integerMode }` to opt in to a different representation:

```ts
// Default: bigint (lossless, NOT JSON-safe)
const rowsBigint = result.toObjects();

// JSON-safe number. Loses precision for |x| > Number.MAX_SAFE_INTEGER
// (i.e. 2**53 - 1). Safe for typical count() / bounded counters.
const rowsNumber = result.toObjects({ integerMode: "number" });
JSON.stringify(rowsNumber); // ok

// Decimal string. Always safe, always JSON-safe.
// Recommended for unbounded I64 identifiers.
const rowsString = result.toObjects({ integerMode: "string" });
```

The option only affects `int` / `uint` columns; other types are unchanged.

## Table Helper

```ts
import { Client } from "scopedb";

const table = client.table("events", {
  database: "scopedb",
  schema: "public",
});

const description = await table.describe();
console.log(description.columns);
```

## Streaming Writes with NDJSON

The streaming write API accepts newline-delimited JSON. The table helper
uses `scopedb` and `public` when the database or schema is not specified.
The destination table must already exist. Use an explicit disposable table for
the snippets before pointing any write path at production.

```ts
import { Client } from "scopedb";

const table = client.table("sdk_example_events", {
  database: "scopedb",
  schema: "public",
});

const result = await table.append(
  [
    JSON.stringify({ id: 1, name: "first" }),
    JSON.stringify({ id: 2, name: "second" }),
  ].join("\n"),
);

console.log(result.num_rows_inserted);
```

For continuous producers, use the asynchronous append stream. It serializes
each record as one NDJSON line, batches by size or time, applies byte-based
backpressure, and sends a bounded number of append requests concurrently.

```ts
const stream = table
  .appendStream()
  .targetBatchBytes(4 * 1024 * 1024)
  .maxBatchRows(10_000)
  .flushIntervalMs(1_000)
  .maxConcurrentBatches(4)
  .maxBufferedBytes(64 * 1024 * 1024)
  .build();

const accepted = await stream.sendAll([
  { id: 1, name: "first" },
  { id: 2, name: "second" },
]);
console.log(accepted.acceptedRows);

// A commit barrier for all rows accepted before flush().
await stream.flush();

// Flushes remaining rows and waits for all in-flight requests. Repeated calls
// return the same promise.
await stream.shutdown();
```

`send()` waits only for local admission capacity; it does not wait for a remote
commit. `sendAll()` consumes an iterable or async iterable one row at a time
with the same admission backpressure. Avoid creating one promise per row with
`Promise.all()`: those promises and their serialized rows can outgrow the
stream's bounded buffer. `flush()` and `shutdown()` are the remote delivery
barriers.

More precisely, a successful barrier in the default `"stop"` mode confirms
that its accepted prefix committed. In `"continue"` mode it is a settlement
barrier: inspect its report because some batches may be rejected, unknown, or
dropped while later batches continue.

The default `failurePolicy` is `"stop"`, which preserves fail-fast behavior
and returns the existing `AppendRowsResult | null` from barriers. Best-effort
telemetry must opt in when creating the stream with
`.appendStream({ failurePolicy: "continue" })`. Its barriers return an
`AppendDeliveryReport` with committed, failed, unknown, and locally dropped row
counts. `failedRows` includes explicitly rejected rows and rows that a local
fatal stream failure prevented from being delivered; ambiguous outcomes remain
separate in `unknownRows`. `outcome` is `"partial"` when at least one row
committed but others were lost or remain unknown. With no committed rows it is
`"unknown"` if any batch may have committed, otherwise `"failed"`; only a
loss-free report is `"ok"`. Never blindly replay an `"unknown"` report. In every
completed report:

```text
acceptedRows = committedRows + failedRows + unknownRows
```

The stream automatically retries only the exact HTTP batch when its temporary
error is explicitly marked `append_state: "rejected"`. That does not make the
whole stream or source safe to replay: other concurrent batches may already be
committed. A transport error or attempt timeout is `unknown`; the SDK reports
that batch without retrying it, then continue mode can process later batches.
Continue mode releases a failed batch after reporting it; it is not an in-memory
retry queue. Use an external spool/outbox when the payload must remain available
for replay or reconciliation. Safe retries honor `Retry-After`, capped by the
configured maximum backoff.

### Choose a delivery path

| Workload | Admission and delivery | Example |
| --- | --- | --- |
| One exact NDJSON payload | Caller owns request boundaries | [`append.ts`](examples/append.ts) |
| Basic asynchronous batching | SDK owns batch boundaries; default strict barriers | [`append-stream.ts`](examples/append-stream.ts) |
| Backfill or file import | Bounded backpressure and concurrent strict batches | [`bulk-import.ts`](examples/patterns/bulk-import.ts) |
| Long-running logs and events | Continue-mode stream with observable loss | [`telemetry.ts`](examples/patterns/telemetry.ts) |
| Fetch-style Serverless | Warm stream settled through a lifecycle hook | [`serverless.ts`](examples/templates/serverless.ts) |
| Durable audit records | One durable attempt per request; ambiguous commits require reconciliation | [`audit-outbox.ts`](examples/templates/audit-outbox.ts) |

For long-running telemetry, `trySend()` attempts local admission without
waiting; a `true` result still does not mean a remote commit. A `false` result
can mean a full buffer, open circuit, invalid or oversized input, or a closed
stream; `stats().droppedByReason` separates those causes. Continue mode's
default circuit opens after five consecutive availability failures and probes
again after 30 seconds. Its default attempt timeout is also 30 seconds.

For Serverless, register the real `flush()` promise with a lifecycle hook such
as `waitUntil()`; a per-attempt `attemptTimeoutMs()` does not bound the whole
barrier or a shared backlog. A report from a module-level stream can cover
concurrent invocations, so it is not an attribution receipt for one event.

For audit data, an in-memory stream is not a durable queue and stable IDs do not
automatically provide idempotency. One durable outbox checkpoint should map to
one size-validated NDJSON request unless the application stores per-request
receipts. An `unknown` result may already have committed and must not be blindly
replayed. Persist `READY -> ATTEMPTING` before the request; after a crash, route
an incomplete `ATTEMPTING` record to reconciliation instead of appending it
again.

An `AbortSignal` passed to `send()` or `sendAll()` cancels only rows still
waiting for local admission. Already accepted rows remain in the stream. For
`flush()` and `shutdown()`, aborting stops the caller's wait but does not cancel
an in-flight append, because doing so would create another unknown outcome.
Lifetime results remain available from `stats()`; the latest completed
continue-mode barrier is also exposed as `stats().lastReport`.

If `sendAll()` is cancelled or its input iterator throws, previously accepted
rows are not rolled back and may already have been dispatched. Call
`shutdown()` when the accepted prefix should still commit; there is no
transactional stream-wide abort or rollback.

The default number of concurrent batches is 4. Set
`.maxConcurrentBatches(1)` when batches must be submitted serially; concurrent
batches do not have a defined commit order. Every table append request is
limited to 8 MiB of uncompressed NDJSON and 200,000 rows; `AppendStream` splits
automatically at either limit.

Remote append failures and ambiguous commit outcomes throw `AppendRowsError`.
Its `appendState`, `rowErrors`, and `rowErrorsTruncated` fields preserve the
structured response. An `appendState` of `"unknown"` means the commit outcome
cannot be determined; retrying the same payload may insert duplicates. The
stream retries only the exact temporary HTTP batch that the server explicitly
marks as `"rejected"`.

## Browse the Catalog

The RESTful catalog methods return database, schema, table-summary, and full
table resources. Use async iterators for the common path; list methods remain
available when an application needs explicit page boundaries.

```ts
for await (const database of client.iterateDatabases({ pageSize: 100 })) {
  console.log(database.name);
}

for await (
  const table of client.iterateTables({
    database: "scopedb",
    schema: "public",
    pageSize: 100,
  })
) {
  console.log(table.name);
}
```

## Errors

Server messages pass through unchanged. `ScopeDBError` adds structured
diagnostics without requiring callers to parse the message:

```ts
import { ScopeDBError } from "scopedb";

try {
  await client.query("SELECT 1");
} catch (error) {
  if (error instanceof ScopeDBError) {
    console.error({
      message: error.message,
      httpStatus: error.httpStatus,
      requestId: error.requestId,
      retryable: error.retryable,
      retryAfterMs: error.retryAfterMs,
      statementDetails: error.statementDetails,
    });
  }
  throw error;
}
```

When a statement reaches the `failed` state, `statementDetails` preserves the
server-provided `code`, `message`, and optional code-specific `details` object.

## CommonJS applications

The package is ESM-only. CommonJS applications can load it with dynamic import:

```js
async function main() {
  const { Client } = await import("scopedb");
  // ...
}

void main();
```

## Batched JSON Ingest

This is also a write path: create the target first and use a disposable table
while evaluating the example.

```ts
import { Client } from "scopedb";

const client = new Client(process.env.SCOPEDB_ENDPOINT!, {
  apiKey: process.env.SCOPEDB_API_KEY!,
});

const stream = client
  .ingestStream(`
    SELECT
      $0["ts"]::timestamp AS occurred_at,
      $0["name"]::string AS name
    INSERT INTO public.sdk_example_events (occurred_at, name)
  `)
  .build();

await stream.send({
  ts: "2026-03-13T12:00:00Z",
  name: "scopedb",
});

await stream.flush();
await stream.shutdown();
```

## Examples

See the runnable instructions and delivery rules in
[`examples/README.md`](examples/README.md).

All examples import the public `scopedb` package entry and are checked with:

```sh
pnpm run check:examples
```

## Development

```sh
pnpm test
pnpm run build
pnpm run check
```

## Delivery Notes

- The package is TypeScript-first and emits declarations from `src/index.ts`.
- Generated artifacts stay out of git; `dist/`, `dist-test/`, and
  `node_modules/` are ignored.
- `prepack` runs unit, type, example, and package-entry checks before creating
  a publishable tarball.
