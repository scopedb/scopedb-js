# ScopeDB SDK for JavaScript

[![Version](https://img.shields.io/npm/v/scopedb)](https://www.npmjs.com/package/scopedb) [![Docs](https://img.shields.io/badge/docs-scopedb.io-blue)](https://www.scopedb.io/docs/developer/nodejs) [![CI](https://img.shields.io/github/actions/workflow/status/scopedb/scopedb-js/ci.yml?branch=main&label=CI)](https://github.com/scopedb/scopedb-js/actions/workflows/ci.yml) [![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://www.apache.org/licenses/LICENSE-2.0)

The official server-side JavaScript client for [ScopeDB](https://www.scopedb.io), with TypeScript support. Run ScopeQL queries, browse the catalog, and write rows with bounded asynchronous streams. For application writes, start with `table.appendStream()`.

The package is ESM-only and has no runtime dependencies.

This development version requires ScopeDB 0.6.0 or later and uses its native Statement, declarative Catalog, and table ingest APIs. These changes are not yet published.

## Runtime support

| Environment        | Recommended usage                                                                                |
| ------------------ | ------------------------------------------------------------------------------------------------ |
| Node.js 20+        | Native ESM; Node 20 is a compatibility floor, so use a maintained Node LTS release in production |
| Next.js            | Route Handlers, Server Actions, and other `server-only` modules; prefer the Node runtime         |
| Bun                | Use the same ESM API and `bun add scopedb`; no Bun-specific adapter                              |
| Cloudflare Workers | Use secret bindings and Web APIs; `nodejs_compat` is not required                                |
| CommonJS           | Load the ESM package with dynamic `import()`                                                     |

Do not use this SDK from browser code, Next.js Client Components, or other untrusted clients. A ScopeDB API key grants server access and must not be included in a browser bundle. The [Next.js](examples/frameworks/nextjs-route-handler/route.ts) and [Cloudflare Worker](examples/frameworks/cloudflare-worker/worker.ts) templates show the intended boundary.

## Installation

```sh
npm install scopedb
# or: pnpm add scopedb
# or: bun add scopedb
```

## Quickstart

Set `SCOPEDB_ENDPOINT` and `SCOPEDB_API_KEY` using the [connection guide](https://www.scopedb.io/docs/guides/connect-to-scopedb). Save this program as `connect.mjs` and run `node connect.mjs`:

```js
import { Client } from "scopedb";

const endpoint = process.env.SCOPEDB_ENDPOINT;
const apiKey = process.env.SCOPEDB_API_KEY;
if (!endpoint || !apiKey) {
  throw new Error("Set SCOPEDB_ENDPOINT and SCOPEDB_API_KEY.");
}

const client = new Client(endpoint, { apiKey });
const result = await client.query("SELECT 1 AS ready");
console.log(result.toObjects());
```

## Documentation

- [SDK guide](https://www.scopedb.io/docs/developer/nodejs)
- [Examples](examples/README.md)
- [Changelog](CHANGELOG.md)
- [Report an issue](https://github.com/scopedb/scopedb-js/issues)

This SDK executes ScopeQL statements; the language is documented separately:

- [Quickstart](https://www.scopedb.io/docs/guides/quickstart)
- [Query guide](https://www.scopedb.io/docs/guides/query-events)
- [Language reference](https://www.scopedb.io/docs/reference)

## Client configuration

The SDK compresses Statement and Append request bodies with gzip using the Web Compression API. Catalog mutations use plain JSON. Append limits are based on the uncompressed NDJSON body.

## Run a statement

Use `client.query()` to submit a statement and wait for its result.

For a detached or long-running statement, keep its handle and choose between a local snapshot, one remote status request, or waiting for the result:

```ts
const handle = await client.statement("SELECT 1 AS ready").submit();

// Synchronous and local: the snapshot updated by submit(), refresh(), wait(), or cancel().
console.log(handle.snapshot()?.status);

// Asynchronous: requests the latest remote status while the statement is active.
const latest = await handle.refresh();
console.log(latest.status);

// Polls until the statement terminates and returns its result set.
const result = await handle.wait();
```

`refresh()` returns the cached snapshot without another request once the handle has reached a terminal state. Use `client.statementHandle(id)` to resume this lifecycle from a previously stored statement ID. `wait()`, `query()`, and `execute()` accept `WaitOptions` when polling delays or cancellation need to be configured.

## Integer representation

`int` and `uint` cells default to JS `bigint` to preserve the full signed and unsigned 64-bit ranges. This is the safe default but is **not** directly JSON-serializable — `JSON.stringify(rowWithBigInt)` throws `TypeError: Do not know how to serialize a BigInt`.

`toValues()`, `toObjects()`, and `first()` accept an optional `{ integerMode }` to opt in to a different representation:

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

## Table helper

```ts
import { Client } from "scopedb";

const table = client.table("events", {
  database: "scopedb",
  schema: "public",
});

const resource = table.catalog();
const snapshot = await resource.get();
console.log(snapshot.definition.columns);

snapshot.definition.comment = "Application events";
const preview = await resource.apply(snapshot.definition, {
  dryRun: true,
  ifMatch: snapshot.etag,
});
await resource.apply(snapshot.definition, { ifMatch: preview.etag });
```

## Append rows

Each append attempt and Catalog mutation defaults to a 30-second SDK deadline. Its budget starts after local encoding, compression, and request preparation and includes reading the complete response. `ClientOptions.writeTimeoutMs` sets the default; direct writes and Catalog mutations accept `timeoutMs`, and streams accept `.attemptTimeoutMs()`. An omitted setting inherits the default; a positive integer overrides it; `null` disables that SDK deadline. Zero is invalid. Caller `AbortSignal` and transport deadlines remain effective. The stream's separate `.maxElapsedTimeMs()` delivery budget still includes all attempts and retry delays, even when its per-attempt deadline is disabled.

A timeout or in-flight cancellation does not roll back a write. Append errors retain their cause and HTTP diagnostics, with `writeDetails: { reason: "timeout" | "cancelled", outcome: "unknown" }`; Catalog mutations report `code: "catalog_commit_outcome_unknown"`. Cancellation before sending preserves the caller's abort reason. Streams never replay client timeouts or cancellations, including when unknown replay is enabled.

Streams retry confirmed transient rejections by default. An unknown outcome stops that batch without replay because the original attempt may have committed. Set `.retryUnknown(true)` only when the application accepts duplicates or can reconcile them; explicit server retry refusal is always honored. `.maxRetries(0)` disables all retries. Direct `append()` and Catalog mutations make one attempt.

HTTP redirects retain the fetch transport's default behavior. A custom `ClientOptions.fetch` can implement an application-specific redirect policy. Redirects are distinct from the SDK's append retry policy and share the attempt deadline.

Use an append stream to write JavaScript objects to an existing table. The SDK groups them into batches automatically. This example uses a table named `sdk_example_events` with `id int` and `name string` columns:

```ts
const table = client.table("sdk_example_events", {
  database: "scopedb",
  schema: "public",
});
const stream = table.appendStream().build();

await stream.send({ id: 1, name: "first" });
await stream.sendAll([
  { id: 2, name: "second" },
  { id: 3, name: "third" },
]);

// Optional: wait for the current writes while keeping the stream open.
// await stream.flush();

// When finished, wait for remaining writes and close the stream.
await stream.shutdown();
```

`send()` and `sendAll()` add records to the SDK's pending writes. Call `shutdown()` when you are done sending to wait for writing to finish. `sendAll()` accepts an iterable or async iterable. Use `flush()` when you need to wait for the current writes and then continue using the stream.

### JSON encoding for Append

Each nonblank NDJSON line must be a JSON object keyed by destination column name. Append does not run a ScopeQL projection or general-purpose casts. These are the non-null input encodings accepted by ScopeDB 0.6.0:

| Column type | JSON input                              | Example and constraints                                                                                                                                                                                                                                  |
| ----------- | --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `int`       | Integer number token                    | `42`, `-42`; signed 64-bit range. `"42"`, `42.0`, and `42e0` are not integer tokens.                                                                                                                                                                     |
| `uint`      | Nonnegative integer number token        | `42`; this NDJSON decoder accepts only `0` through `9223372036854775807`, even though query results support the full unsigned 64-bit range.                                                                                                              |
| `float`     | Finite number                           | `1.5`, `1e3`; integer tokens are accepted only when exactly representable as a float. Overflow, nonzero values that underflow to zero, NaN, and infinities are rejected.                                                                                 |
| `boolean`   | Boolean                                 | `true` or `false`, without quotes.                                                                                                                                                                                                                       |
| `string`    | String                                  | `"hello"`; numbers and objects are not implicitly converted to text.                                                                                                                                                                                     |
| `timestamp` | RFC 3339 string with an explicit offset | `"2026-10-23T12:34:56.123456789Z"` or `"2026-10-23T20:34:56+08:00"`. Use `T` between date and time and `Z` or `±HH:MM`; date-only, timezone-free, and numeric epoch inputs are rejected. Values must fit signed 64-bit nanoseconds since the Unix epoch. |
| `interval`  | Fixed-duration ISO 8601 `PT` string     | `"PT1H2M3.000000004S"`, `"-PT0.5S"`; hours, minutes, and seconds, with up to nine fractional second digits and signed 64-bit nanosecond range. Calendar years/months/days are not accepted.                                                              |
| `binary`    | Canonical padded standard Base64 string | `"aGVsbG8="`; URL-safe or unpadded alternatives are rejected when they differ from canonical encoding.                                                                                                                                                   |
| `object`    | JSON object                             | `{"nested":{"enabled":true}}`; pass the object itself, not a string containing serialized JSON.                                                                                                                                                          |
| `array`     | JSON array                              | `[1,"two",null,{"ok":true}]`; elements may have different JSON types.                                                                                                                                                                                    |
| `any`       | Any JSON value                          | Numbers, booleans, strings, arrays, and objects retain their JSON kinds; a timestamp-looking string remains a string.                                                                                                                                    |

Missing ordinary columns and explicit `null` become SQL NULL for every column type. Nested nulls inside objects/arrays remain JSON nulls. Computed columns are evaluated by the table and must be omitted from input. Unknown fields, duplicate object keys (including nested keys), and type mismatches reject the entire append request; other requests in a stream may already have committed. Inspect `AppendRowsError.outcome`, `code`, and `rowErrors`; row diagnostics may be truncated.

The signed 64-bit integer-token limit also applies inside `object`, `array`, and `any`. `appendStream()` uses JavaScript's `JSON.stringify`: unsafe integers may already be rounded before the SDK sees them, `bigint` throws, NaN/Infinity become `null`, and `undefined` object properties are omitted. Validate numbers with `Number.isFinite` and use `Number.isSafeInteger` when supplying integer-valued JavaScript numbers. For larger exact integers, preserve validated NDJSON text and call `append()`, as shown in [import-ndjson.ts](examples/import-ndjson.ts). Do not turn them into quoted numeric strings for an integer column.

For a JavaScript `Date` within the timestamp range above, `toISOString()` produces an accepted UTC timestamp. It has millisecond precision; retain an already valid RFC 3339 source string when sub-millisecond precision matters. Parse and validate any source text that contains serialized objects before passing the resulting object to Append.

### Continue after a failed batch

To keep processing later batches when one fails, set `failurePolicy` to `"continue"` and check the report returned by `flush()` or `shutdown()`:

```ts
const stream = table.appendStream({ failurePolicy: "continue" }).build();

await stream.sendAll([
  { id: 4, name: "fourth" },
  { id: 5, name: "fifth" },
]);

const report = await stream.shutdown();
console.log(report);
```

### Long-lived telemetry and rate limiting

For a best-effort telemetry sink that must keep accepting later events after a failed batch, start with one stream per destination in each process and an explicit continue policy:

```ts
const telemetry = table.appendStream({ failurePolicy: "continue" })
  .targetBatchBytes(1024 * 1024)
  .flushIntervalMs(1_000)
  .maxConcurrentBatches(1)
  .maxBufferedBytes(8 * 1024 * 1024)
  .attemptTimeoutMs(10_000)
  .maxElapsedTimeMs(30_000)
  .maxRetries(2)
  .circuitBreaker({ failureThreshold: 5, cooldownMs: 30_000 })
  .onBatchFailure(({ outcome, batchRows, error }) => {
    // Send diagnostics to a different sink to avoid recursive telemetry failures.
    console.error("telemetry batch failed", { outcome, batchRows, error });
  })
  .build();

telemetry.trySend({ id: 1, name: "request.completed" });
// On graceful shutdown: stop producers, then await telemetry.shutdown().
```

`trySend()` returns false for local admission failures and records the reason in `stats()`. Continue mode does not retain failed payloads for later replay. Its circuit breaker opens after consecutive availability failures, rejects new admission during cooldown, and allows a probe afterwards; a permanent schema error does not open the breaker. The default breaker settings are five failures and a 30-second cooldown. Use a durable outbox when losing telemetry is unacceptable. See the runnable [telemetry example](examples/patterns/telemetry.ts).

| Metric                   | `stats()` field                                                  | Interpretation                                                                                                                                                                                                                                                                               |
| ------------------------ | ---------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Dropped before admission | `droppedRows`, `droppedByReason`                                 | Failed `trySend()` calls, split into buffer full, invalid record, oversized record, open circuit, and closed stream. Thrown `send()`/`sendAll()` admission errors must be counted by the caller.                                                                                             |
| Known not committed      | `failedRows`                                                     | Admitted rows whose batch ended in rejection; also includes admitted but unsent rows discarded after a fatal stream failure. For specifically server-rejected batches, count `batchRows` in `onBatchFailure` when `outcome === "rejected"`, using HTTP metadata to distinguish local errors. |
| Possibly committed       | `unknownRows`                                                    | Admitted rows with an unresolved commit outcome; keep separate from definite loss and never blindly replay them.                                                                                                                                                                             |
| Confirmed committed      | `committedRows`                                                  | Rows acknowledged as committed.                                                                                                                                                                                                                                                              |
| Retry pressure           | `retries`, `lastFailure.error`                                   | Extra attempts and complete final failure diagnostics, including HTTP status and `retryAfterMs`.                                                                                                                                                                                             |
| Current backlog          | `pendingRows`, `pendingBytes`, `inFlightBatches`, `circuitState` | Gauges for backpressure, requests in flight, and circuit state.                                                                                                                                                                                                                              |

Row/retry counters are cumulative for the stream's lifetime and are not reset by `flush()`. Export their deltas or maintain a baseline per stream. `flush()`/`shutdown()` delivery reports cover their reporting interval; do not add them to the lifetime counters again.

The SDK default remains four concurrent batches **per stream**, with no process-wide or fleet-wide coordinator. Twelve processes with one default stream each can issue up to 48 concurrent append requests; the recipe above reduces that bound to 12. Multiple streams per process multiply it again. Start at one batch per stream for a fleet that is already seeing 429s, then increase only after observing throughput, 429s, retries, and backlog. Concurrency limits requests in flight, not requests per second or total ingestion volume; reduce producer rate or coordinate admission across processes if the shared limit is still exceeded. Confirmed transient rejections use jittered backoff and honor `Retry-After`; a retry that cannot fit the delivery budget is not sent early. A 429 without a trustworthy rejected outcome remains unknown and is not replayed by default.

### Append an NDJSON string

If you already have newline-delimited JSON, pass it to `table.append()`:

```ts
const result = await table.append('{"id":6,"name":"sixth"}\n');
console.log(result.num_rows_inserted);
```

### Caller-managed retries

If the application owns the retry loop, require both a confirmed rejection and a temporary classification. An HTTP status or `retryable` flag alone is insufficient. This Node.js example makes at most three attempts, honors `Retry-After`, and uses the caller's signal to bound the whole operation:

```ts
import { setTimeout as delay } from "node:timers/promises";
import { AppendRowsError, type Table } from "scopedb";

async function appendWithRetry(table: Table, ndjson: string, signal: AbortSignal) {
  for (let retries = 0; ; retries++) {
    signal.throwIfAborted();
    try {
      return await table.append(ndjson, { signal });
    } catch (error) {
      if (!(error instanceof AppendRowsError) || error.outcome !== "rejected" ||
          !error.isTemporary() || error.serverRetryable === false ||
          error.writeDetails !== undefined || retries >= 2) {
        throw error;
      }
      const backoffMs = 200 * 2 ** retries * (0.5 + Math.random() / 2);
      const waitMs = Math.max(backoffMs, error.retryAfterMs ?? 0);
      // Give long server delays back to the caller; never shorten Retry-After.
      if (waitMs > 30_000) throw error;
      await delay(waitMs, undefined, { signal });
    }
  }
}

await appendWithRetry(table, '{"id":6,"name":"sixth"}', AbortSignal.timeout(30_000));
```

An unknown result, including `writeDetails.outcome === "unknown"` after timeout or cancellation, may already be committed: stop and reconcile it. A subsequent definite rejection cannot resolve an earlier unknown attempt. Avoid layering an application retry loop around a retrying stream. A type-checked copy is in [retry-append.ts](examples/templates/retry-append.ts).

## Browse the catalog

Catalog reads return complete declarative definitions and opaque ETags. Select the smallest subtree you need. Apply replaces that whole subtree, including its children; omitted fields are cleared. Use a dry run and an ETag precondition to review changes and reject concurrent updates.

```ts
const resource = client.catalog().table("scopedb", "public", "events");
const snapshot = await resource.get();
const definition = { ...snapshot.definition, comment: "Application events" };
const preview = await resource.apply(definition, {
  dryRun: true,
  ifMatch: snapshot.etag,
});
console.log(preview.before, preview.after, preview.destructive);
await resource.apply(definition, { ifMatch: preview.etag });
```

Potentially destructive changes require `allowDestructive: true`. Mutations never retry automatically and use the client's `writeTimeoutMs` by default; `CatalogOptions.timeoutMs` overrides it. An interrupted or malformed mutation response has code `catalog_commit_outcome_unknown`; read the current definition before deciding whether to replay. Keep the ETag unchanged, including its quotes. Definitions use the server's snake_case field names and retain unknown fields when read and reapplied.

## Errors

Server messages pass through unchanged. `ScopeDBError` adds structured diagnostics without requiring callers to parse the message:

```ts
import { ScopeDBError } from "scopedb";

try {
  await client.query("SELECT 1");
} catch (error) {
  if (error instanceof ScopeDBError) {
    console.error({
      code: error.code,
      details: error.details,
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

`stats().lastFailure.error` retains the full batch error, including timeout diagnostics. When a statement reaches the `failed` state, `statementDetails` preserves the server-provided `code`, `message`, and optional code-specific `details` object.

## CommonJS applications

The package is ESM-only. CommonJS applications can load it with dynamic import:

```js
async function main() {
  const { Client } = await import("scopedb");
  // ...
}

void main();
```

## Migrate to ScopeDB 0.6

This is a breaking release requiring ScopeDB 0.6.0 or later. Upgrading the server from 0.5 clears statement history and saved results; previously saved handles cannot be resumed.

### Existing clients and new APIs

ScopeDB 0.6.0 retains `POST /v1/ingest`, including the data-and-ScopeQL transform request used by JavaScript SDK 0.2.x `insert()` and `ingestStream()`. Existing writers can continue using that endpoint after the server upgrade. New SDK development uses native Append, declarative Catalog, and structured Statement APIs; this SDK requires 0.6.0 and has no 0.5 compatibility mode.

Raw HTTP Statement clients can still send `exec_timeout` as a duration string such as `"30s"` or `"1m"`. ScopeDB 0.6.0 maps it to `max_elapsed_seconds`; it is not ignored. Sending both fields returns HTTP 400, as do invalid or nonpositive durations. Use `max_elapsed_seconds: 30` in new clients; `max_estimated_seconds` is an independent optional limit. The elapsed limit includes queueing time.

### SDK API migration

| Previous API                                                                              | Replacement                                                                                           |
| ----------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `withExecTimeout("30s")`                                                                  | `withMaxElapsedSeconds(30)`; optionally `withMaxEstimatedSeconds(5)`                                  |
| Catalog `list*`, `fetch*`, `iterate*`                                                     | Select the resource through `client.catalog()`, then `get()`                                          |
| `table.describe()`, `tableSchema()`, `apply()`, `drop()`                                  | `table.catalog().get()`, `.apply()`, `.delete()`                                                      |
| `lastStatus()`, `status()` / `fetchOnce()`, `fetch()`                                     | `snapshot()`, `refresh()`, `wait()`                                                                   |
| Handle `progress()` / `resultSet()`                                                       | Inspect the complete snapshot; use `wait()` for a cached completed result                             |
| `StatementStatus*`, `FetchOptions`                                                        | `StatementSnapshot*`, `WaitOptions`                                                                   |
| Append result `append_state`, error `appendState`                                         | Result `status`, error `outcome`                                                                      |
| `stats().lastFailure.message` and partial diagnostics                                     | `stats().lastFailure.error`, including code, details, HTTP metadata, timeout reason, and cause        |
| `token`                                                                                   | `apiKey`                                                                                              |
| `table.withDatabase()` / `withSchema()`                                                   | `client.table(name, { database, schema })`                                                            |
| `jsonRows()`, `intoValues()`, `intoObjects()`, `IntoOptions`                              | `rawRows()`, `toValues()`, `toObjects()`, `ResultOptions`                                             |
| `executeOne()`                                                                            | `(await statement.execute()).first()`                                                                 |
| `batchBytes`, `flushInterval`, `maxPendingBytes`, `maxInFlightRequests`, `attemptTimeout` | `targetBatchBytes`, `flushIntervalMs`, `maxBufferedBytes`, `maxConcurrentBatches`, `attemptTimeoutMs` |
| `u_int`                                                                                   | `uint`                                                                                                |
| Low-level `Client` transport methods                                                      | `Table.append()`, `Statement.submit()`, `StatementHandle.refresh()` / `cancel()`                      |

Statement snapshots include identity, status, progress, and either the complete result or original failure. `snapshot()` is local; `refresh()` observes the server; both return caller-owned copies. Concurrent refreshes and cancellation cannot replace a terminal state with an older response. `cancel()` can return pending/running, finished, failed, or cancelled: inspect the observation. `wait()` reports confirmed server cancellation as `ScopeDBError.kind === "StatementCancelled"`; aborting its signal only stops local waiting. Failed statements retain `code`, `message`, `details`, and unknown future error codes. Client query waiting has no additional deadline unless the caller supplies a signal; server execution limits still apply.

Catalog reads retain the complete definition and opaque ETag. Carry that ETag through dry-run and apply/delete to reject intervening changes. Replacements clear omitted fields and children. Known fields are validated, while extension fields survive a read/modify/write cycle. Table identifiers now always include database/schema defaults, matching the Catalog and Append target.

Table writes use the native catalog ingest endpoint. Unknown outcomes require explicit replay opt-in; direct writes remain single attempts. Integer results default to exact `bigint`, with decimal strings available for JSON consumers. NDJSON input on ScopeDB 0.6 is limited to signed 64-bit integer literals even for `uint` columns; query results support the full unsigned range. Preserve caller-encoded NDJSON text when importing large integers instead of parsing and reserializing through JavaScript numbers.

## Migrate from Ingest

`Client.insert()`, `Client.ingest()`, `Client.ingestStream()`, `IngestStream`, `IngestStreamBuilder`, and the ingest protocol types have been removed. Use `client.table(name).append(ndjson)` for one request or `.appendStream().build()` to batch JavaScript objects. This SDK version requires ScopeDB 0.6.0 or later.

Append accepts rows matching the destination columns, not a ScopeQL statement. Move field renaming, type conversion, filtering, and routing into the producer before calling `send()`, using the [Append JSON encoding rules](#json-encoding-for-append). For a small producer-side mapping:

```ts
const source = { ts: "2026-03-13T12:00:00Z", name: "scopedb" };
const stream = client.table("events").appendStream()
  .maxConcurrentBatches(1)
  .maxRetries(0)
  .build();
try {
  await stream.send({ occurred_at: new Date(source.ts).toISOString(), name: source.name });
} finally {
  const report = await stream.shutdown();
  console.log("Committed rows:", report?.num_rows_inserted ?? 0);
}
```

For transformations that must run in ScopeQL, append the original event into an `object` column in a staging table, then execute a separate `FROM ... SELECT ... INSERT INTO ...` statement. Staging and transformation are separate commits; track processed batches before retrying the transformation.

Review these behavior changes when migrating:

- Append batches allow up to 8 MiB of uncompressed NDJSON and 200,000 rows. Reduce an old ingest batch-byte target above 8 MiB.
- `flush()` and `shutdown()` in the default stop policy return `AppendRowsResult | null`: read `num_rows_inserted`, with `null` for an empty barrier. The continue policy returns an `AppendDeliveryReport` for activity since the previous report; check `outcome` and `committedRows`. Direct `append()` returns `AppendRowsResult`.
- Append streams send up to four batches concurrently by default. Use `.maxConcurrentBatches(1)` for sequential requests; queries still require `ORDER BY` for ordered results.
- Append streams only replay unknown outcomes with `.retryUnknown(true)`, which can duplicate rows. Use `.maxRetries(0)` to disable retries of rejected batches as well. Client timeouts and cancellations are never retried. Direct `append()` makes one attempt.

## Examples

See the runnable instructions in [`examples/README.md`](examples/README.md).

All examples import the public `scopedb` package entry and are checked with:

```sh
pnpm run check:examples
```

## Development

Use the pnpm version declared in `package.json`:

```sh
pnpm install --frozen-lockfile
pnpm test
pnpm run check
```

The package emits TypeScript declarations from `src/index.ts`. Generated artifacts stay out of git, and `prepack` runs the unit, type, example, and package-entry checks before creating a publishable tarball.

Release history and the maintainer runbook are in [CHANGELOG.md](CHANGELOG.md) and [DELIVERY.md](DELIVERY.md).

## License

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).

## Live protocol validation

From a source checkout, set `SCOPEDB_ENDPOINT` and `SCOPEDB_API_KEY` to an authenticated ScopeDB 0.6 endpoint, then run:

```sh
pnpm test:integration
```

The test creates a uniquely named schema and checks dry-run, ETag conflicts, committed and rejected appends, statement errors, elapsed timeout, and a cancellation racing with completion. It deletes its schema before returning. Running cancellation is covered separately with controlled HTTP responses in the unit tests.
