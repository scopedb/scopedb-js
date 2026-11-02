# Using the JavaScript SDK

The SDK has three entry points: run ScopeQL with `client.query()`, write records with `client.table()`, and inspect or change definitions with `client.catalog()`. A `Client` can be reused across requests. Table and statement builders select an operation; they do not open a database connection.

Follow the [quickstart](../README.md#run-a-query) for your first query. This guide covers the choices you make when integrating the SDK into an application:

- [Connect to ScopeDB](#connect-to-scopedb): credentials, table locations, and custom HTTP transports.
- [Read query results](#read-query-results): JavaScript values, precision, and JSON output.
- [Track or cancel a query](#track-or-cancel-a-query): execution limits and recovery without resubmission.
- [Append rows](#append-rows): choose a write path, settle writes, and handle partial delivery.
- [Read or change table schemas](#read-or-change-table-schemas): inspect, preview, and apply definitions.
- [Timeouts and HTTP options](#timeouts-and-http-options) and [Errors](#errors): bound work and choose a recovery action.

This guide describes the current source, including changes planned for 0.4. For upgrades from the published 0.3 API, see [the migration guide](migration-0.4.md); use the documentation included with your installed package for that release.

## Connect to ScopeDB

Keep the endpoint and API key in your server's configuration. The SDK does not read environment variables itself:

```ts
import { Client } from "scopedb";

const endpoint = process.env.SCOPEDB_ENDPOINT;
const apiKey = process.env.SCOPEDB_API_KEY;
if (!endpoint || !apiKey) {
  throw new Error("Set SCOPEDB_ENDPOINT and SCOPEDB_API_KEY.");
}
const client = new Client(endpoint, { apiKey });
const table = client.table("events", { database: "scopedb", schema: "public" });
```

The examples below use this `client` and an existing `events` table with `id int` and `name string` columns. `table()` defaults to database `scopedb` and schema `public`; it neither creates a table nor changes the database or schema used by ScopeQL queries. Use qualified names in queries when needed. `table.identifier()` returns the correctly quoted, fully qualified table name for constructing a statement; it does not escape arbitrary SQL values.

`ClientOptions` also accepts `headers`, a custom Web-compatible `fetch`, and `writeTimeoutMs`. `apiKey` takes precedence over an `Authorization` header. Requests opt out of framework fetch caches. For Next.js and Workers credential setup, see the [runtime templates](../examples/README.md#framework-and-runtime-templates).

## Read query results

`client.query(scopeql)` submits a statement, waits for completion, and returns all rows in a `ResultSet`. Use ScopeQL projections and `LIMIT` to bound the result you load into memory. By default, `int` and `uint` columns decode to JavaScript `bigint` so signed and unsigned 64-bit values never lose precision. Because `JSON.stringify()` does not serialize `bigint` values directly, `toValues()`, `toObjects()`, and `first()` accept an `{ integerMode }` option:

| `integerMode`        | Return type | When to use                                                                                                        |
| -------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------ |
| `"bigint"` (default) | `bigint`    | Exact signed or unsigned 64-bit integers.                                                                          |
| `"number"`           | `number`    | JSON responses and bounded counters within `Number.MIN_SAFE_INTEGER`..`Number.MAX_SAFE_INTEGER` ($\pm(2^{53}-1)$). |
| `"string"`           | `string`    | JSON responses containing unbounded 64-bit identifiers or counters without precision loss.                         |

```ts
const result = await client.query("SELECT 42 AS count");

// Default: lossless bigint
const rowsBigint = result.toObjects();

// JSON-safe number for bounded counts and small integers
const rowsNumber = result.toObjects({ integerMode: "number" });
JSON.stringify(rowsNumber);

// Lossless decimal string for JSON payloads with 64-bit IDs
const rowsString = result.toObjects({ integerMode: "string" });
```

The `"number"` mode permits rounding outside the safe integer range; it does not reject oversized values. For example, `9007199254740993` becomes `9007199254740992`. Prefer `"string"` when exposing arbitrary integer columns as JSON.

Other column types have these representations:

| ScopeDB value                | JavaScript value              | Application choice                                                                                    |
| ---------------------------- | ----------------------------- | ----------------------------------------------------------------------------------------------------- |
| SQL `NULL`                   | `null`                        | Preserved in every conversion mode.                                                                   |
| `float`, `boolean`, `string` | `number`, `boolean`, `string` | Special floats remain `NaN` / `Infinity`; JSON serialization turns them into `null`.                  |
| `timestamp`                  | `Date`                        | Millisecond precision; use `rawRows()` to retain the original nanosecond timestamp text.              |
| `object`, `array`            | `string`                      | JSON text is preserved. Parse explicitly if needed; `JSON.parse()` can round large nested integers.   |
| `any`                        | `string`                      | The cell text is preserved without inferring its runtime type; it is not necessarily a JSON document. |
| `interval`, `binary`         | `string`                      | Preserved as returned by ScopeDB.                                                                     |

`integerMode` applies only to `int` and `uint` columns, not to values stored in `object`, `array`, or `any` columns. Query values are therefore not a general-purpose Append record format: decode structured cells and choose any precision conversions deliberately before writing them back.

`toObjects()` returns objects keyed by column name; `first()` returns one such object or `null` for an empty result. Both reject duplicate column names. Use unique aliases or `toValues()` for positional cells. `schema().fields()` provides their names and ScopeDB types; `rawRows()` provides the original string-or-null cells.

## Track or cancel a query

For long-running or non-idempotent statements, build a `Statement` with `client.statement(scopeql)` to configure execution limits or manage the execution handle directly:

```ts
const handle = await client.statement("SELECT 1 AS ready")
  .withMaxElapsedSeconds(60)
  .withMaxEstimatedSeconds(5)
  .submit();

// Inspect the last known state without a request.
console.log(handle.snapshot()?.status);

// Refresh progress once.
const latest = await handle.refresh();
console.log(latest.status);

// Wait for the result.
const result = await handle.wait();
```

- `withMaxElapsedSeconds(seconds)` bounds total execution time including queueing; `withMaxEstimatedSeconds(seconds)` rejects queries whose estimated runtime exceeds the threshold.
- `refresh()` and `cancel()` return the cached snapshot without making a network request once the handle reaches a terminal state (`finished`, `failed`, or `cancelled`).
- Passing an `AbortSignal` to `wait()`, `query()`, or `execute()` stops local waiting without cancelling the server statement; call `handle.cancel()` to request server-side cancellation.
- `cancel()` returns the observed state, which may still be running or may already have finished. Inspect that state or call `wait()` for the final result; a cancellation request is not confirmation that a write was rolled back.
- Retain `handle.statementId` to reattach with `client.statementHandle(id)` while the service retains the statement. If polling fails, retry waiting on that handle instead of submitting the SQL again. A query can perform a write, so resubmission can duplicate effects.
- If the initial submission must be recoverable too, assign `crypto.randomUUID()` with `withStatementId(id)` before `submit()`. Keep the ID even when the submission response is lost; inspect that statement before deciding whether to submit again.

## Append rows

Choose based on the input you already have and how your application handles delivery:

| Write path             | Best for                                                                                          |
| ---------------------- | ------------------------------------------------------------------------------------------------- |
| `table.appendStream()` | Event ingestion, bulk imports, and telemetry pipelines that benefit from automatic batching.      |
| `table.append(ndjson)` | Pre-encoded newline-delimited JSON payloads or single-request writes with caller-managed retries. |

```ts
const table = client.table("events", {
  database: "scopedb",
  schema: "public",
});
const stream = table.appendStream().build();

await stream.send({ id: 1, name: "first" });
await stream.sendAll([
  { id: 2, name: "second" },
  { id: 3, name: "third" },
]);

// Optional: wait for buffered rows to commit while keeping the stream open
// await stream.flush();

// Flush remaining buffered rows and permanently close the stream
await stream.shutdown();
```

`send()` and `sendAll()` serialize records and enqueue them into the stream's bounded buffer. Await `flush()` to settle all rows admitted so far while keeping the stream open, or `shutdown()` when finished producing rows. If `sendAll()` fails midway (for example, on an invalid record), earlier rows from the iterable remain admitted in the stream; settle the stream before deciding which remaining input rows to send.

### Delivery and failure policies

A stream buffers rows in memory; it is not a durable queue. A successful `send()` means local acceptance, not delivery. `flush()` waits for the rows admitted before that call; `shutdown()` also closes the stream. Stop producers before shutting down. Aborting a wait stops that caller from waiting; it does not remove accepted rows or undo requests already sent.

| Policy             | When a batch fails                                              | What a completed flush or shutdown means                                                   |
| ------------------ | --------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `"stop"` (default) | The stream stops accepting rows and settles in-flight requests. | Returns `AppendRowsResult \| null` on success; rejects on unrecovered delivery failure.    |
| `"continue"`       | Later batches may still be delivered.                           | Returns an `AppendDeliveryReport`; inspect committed, failed, ambiguous, and dropped rows. |

Results cover the interval since the previous completed flush or shutdown. In stop mode, `null` means there were no rows in that interval. Lifetime totals are available from `stats()`. A cancelled flush is carried into the next report, so its delivery outcomes are not lost.

Atomicity applies to each Append request, not to the whole stream. If a batch is rejected, earlier or concurrent batches may have committed. Settle the stream and inspect its totals before deciding what to replay; a failed shutdown does not make the entire input safe to retry. For recoverable imports, retain a durable source and checkpoints; see [bulk import](../examples/patterns/bulk-import.ts) and [the outbox template](../examples/templates/audit-outbox.ts).

### Batching and retries

Streams automatically retry confirmed temporary HTTP 429 admission refusals, respecting the server's retry refusal and `Retry-After`. They do not replay ambiguous writes by default. `table.append()` always makes a single attempt. Neither path promises exactly-once delivery.

| Builder method                  | Default          | Description                                                                                              |
| ------------------------------- | ---------------- | -------------------------------------------------------------------------------------------------------- |
| `.strictEncoding(enabled)`      | `false`          | Use `encodeAppendRecord()` to support `bigint` and reject lossy numbers or unsupported types.            |
| `.targetBatchBytes(bytes)`      | `4 MiB`          | Target uncompressed NDJSON batch size before sealing a batch (maximum `8 MiB`).                          |
| `.maxBatchRows(rows)`           | `200,000`        | Maximum rows per append request (maximum `200,000`).                                                     |
| `.flushIntervalMs(ms)`          | `1,000`          | Seal a batch after this interval from its first row; dispatch may wait for capacity or circuit recovery. |
| `.maxBufferedBytes(bytes)`      | `64 MiB`         | Total buffered and in-flight byte budget before `send()` applies backpressure or `trySend()` drops.      |
| `.maxConcurrentBatches(count)`  | `4`              | Maximum concurrent HTTP append requests per stream instance.                                             |
| `.attemptTimeoutMs(ms \| null)` | `writeTimeoutMs` | Per-attempt HTTP deadline in milliseconds (`null` disables the per-attempt deadline).                    |
| `.maxElapsedTimeMs(ms)`         | `300,000`        | Total per-batch delivery budget across attempts and retry backoffs.                                      |
| `.maxRetries(count)`            | `8`              | Maximum retries for confirmed temporary admission refusals (`0` disables retries).                       |
| `.retryAmbiguous(enabled)`      | `false`          | Also retry transient `"ambiguous"` outcomes (can duplicate rows if an earlier attempt committed).        |

### JSON encoding for Append

Each row must serialize to a JSON object keyed by destination column name:

| Column type | JSON representation                  | Notes and constraints                                                                                                   |
| ----------- | ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------- |
| `int`       | Integer number token                 | `42`, `-42` within signed 64-bit range; quoted strings (`"42"`) and fractional/exponential forms (`42.0`) are rejected. |
| `uint`      | Nonnegative integer number token     | `0` through `9223372036854775807` in NDJSON input.                                                                      |
| `float`     | Finite number                        | `1.5`, `1e3`; `NaN`, `Infinity`, and `-Infinity` are rejected.                                                          |
| `boolean`   | Boolean                              | `true` or `false`.                                                                                                      |
| `string`    | String                               | UTF-8 text; numbers and objects are not coerced to strings.                                                             |
| `timestamp` | RFC 3339 string with explicit offset | `"2026-10-23T12:34:56.123456789Z"` or `"2026-10-23T20:34:56+08:00"` (`Date.toISOString()` produces millisecond UTC).    |
| `interval`  | ISO 8601 `PT` duration string        | `"PT1H2M3.000000004S"`, `"-PT0.5S"` (hours, minutes, and seconds only; calendar day/month/year units are not used).     |
| `binary`    | Standard padded Base64 string        | `"aGVsbG8="`.                                                                                                           |
| `object`    | JSON object                          | `{"nested":{"enabled":true}}` (pass a nested object, not a JSON-encoded string).                                        |
| `array`     | JSON array                           | `[1,"two",null,{"ok":true}]`.                                                                                           |
| `any`       | Any JSON value                       | Preserves the JSON value kind.                                                                                          |

Omitted columns and `null` values become SQL `NULL`. Computed columns are evaluated by ScopeDB and must be omitted from input records. Append requests are atomic per batch: unknown columns, duplicate keys, or type mismatches reject the entire request with `AppendRowsError` (`outcome: "failed"`).

### Strict object encoding

By default, `appendStream()` serializes objects with `JSON.stringify()`, which throws on `bigint`, turns `NaN`/`Infinity` into `null`, and silently drops `undefined` properties. Enable `.strictEncoding(true)` on a stream or call `encodeAppendRecord(record)` for direct `table.append()` to emit exact 64-bit `bigint` tokens and reject lossy or unsupported values locally before buffering:

```ts
import { encodeAppendRecord } from "scopedb";

const stream = table.appendStream().strictEncoding(true).build();
await stream.send({ id: 9007199254740993n, name: "exact integer" });
await stream.shutdown();

await table.append(encodeAppendRecord({ id: 9007199254740995n, name: "direct" }));
```

Each strict-encoded row must be a plain object. Its fields can contain nested plain objects, arrays, strings, booleans, `null`, valid `Date` instances (serialized via `toISOString()`), finite numbers (integer-valued `number`s must satisfy `Number.isSafeInteger`), and signed 64-bit `bigint`s. It rejects `NaN`, `Infinity`, unsafe integers, `undefined`, functions, symbols, sparse arrays, custom class instances (`Map`, `Set`, `Uint8Array`), accessor properties, and circular references with a permanent `ScopeDBError` (`kind: "AppendRowsFailed"`) that records the offending field path in `error.context().get("record_path")` (such as `$["attrs"][0]["score"]`).

### Continue after a failed batch

Set `{ failurePolicy: "continue" }` when a stream should keep processing subsequent batches after a batch fails. In continue mode, `flush()` and `shutdown()` resolve to an `AppendDeliveryReport` summarizing the interval since the previous completed barrier:

```ts
const stream = table.appendStream({ failurePolicy: "continue" }).build();

await stream.sendAll([
  { id: 4, name: "fourth" },
  { id: 5, name: "fifth" },
]);

const report = await stream.shutdown();
console.log(report.outcome, report.committedRows, report.failedRows, report.ambiguousRows);
```

### Long-lived telemetry and rate limiting

For non-blocking telemetry sinks, combine `failurePolicy: "continue"`, `trySend()`, `onBatchFailure()`, and the built-in circuit breaker:

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
    console.error("telemetry batch failed", { outcome, batchRows, error });
  })
  .build();

telemetry.trySend({ id: 1, name: "request.completed" });
// On graceful shutdown: stop producers, then await telemetry.shutdown().
```

`trySend()` never waits or throws; when it returns `false`, the drop reason is recorded in `telemetry.stats().droppedByReason` (`bufferFull`, `invalidRecord`, `recordTooLarge`, `circuitOpen`, or `closed`). The circuit breaker opens after `failureThreshold` consecutive availability failures, rejects `trySend()` calls while open, and admits a single probe after `cooldownMs`. Lifetime counters on `stats()` (`acceptedRows`, `committedRows`, `failedRows`, `ambiguousRows`, `droppedRows`, `retries`) accumulate over the life of the stream, whereas `flush()` and `shutdown()` reports cover only the interval since the previous completed barrier. See [`examples/patterns/telemetry.ts`](../examples/patterns/telemetry.ts) for a complete runnable example.

### Append an NDJSON string

When you already have newline-delimited JSON (up to `8 MiB` uncompressed and `200,000` rows per call), pass it directly to `table.append()`:

```ts
const result = await table.append('{"id":6,"name":"sixth"}\n');
console.log(result.num_rows_inserted);
```

### Caller-managed retries

Prefer a stream when you want the SDK to manage retries. For pre-encoded NDJSON, the [direct Append retry template](../examples/templates/retry-append.ts) provides a bounded, cancellable loop. It retries only confirmed temporary failures, respects the server's retry refusal and retry delay, and does not replay timeouts or cancellations.

An `AppendRowsError` with `outcome: "failed"` confirms that the failed request committed no rows. `outcome: "ambiguous"` means it may have committed. Reconcile with your durable source or downstream state before replaying an ambiguous request. Enabling `.retryAmbiguous(true)` is appropriate only when duplicates are acceptable or the application can deduplicate them.

## Read or change table schemas

`client.catalog()` and `table.catalog()` read and replace declarative definitions for databases, schemas, tables, and views. Each `get()` returns a complete `{ definition, version }` snapshot. `apply()` replaces the target resource and its children (clearing omitted fields); use `dryRun: true` and `expectedVersion` to preview changes and guard against concurrent edits:

```ts
const resource = client.table("events").catalog();
const snapshot = await resource.get();
const definition = { ...snapshot.definition, comment: "Application events" };

const preview = await resource.apply(definition, {
  dryRun: true,
  expectedVersion: snapshot.version,
});
console.log(preview.before, preview.after, preview.destructive);

await resource.apply(definition, { expectedVersion: preview.version });
```

- Pass `snapshot.version` unchanged as `expectedVersion`. The version token represents the tenant catalog state; concurrent catalog changes cause `apply()` or `delete()` to fail atomically with HTTP `412` (`code: "precondition_failed"`).
- Dropping a resource or making destructive schema changes requires `allowDestructive: true`.
- Catalog definitions use snake_case property names matching the ScopeDB schema model and preserve unknown extension fields across read-modify-write cycles.
- Catalog mutations are single-attempt operations. When a mutation fails, inspect `error.catalogOutcome`: `"rejected"` means the change did not take effect, whereas `"unknown"` means the request was interrupted or failed ambiguously and you should re-read `get()` before retrying.

## Timeouts and HTTP options

Choose the deadline for the operation you want to bound. A per-attempt write timeout starts after local encoding, compression, and request preparation and covers sending the request and reading its response:

| Option / Parameter                     | Default      | Scope                                                                                              |
| -------------------------------------- | ------------ | -------------------------------------------------------------------------------------------------- |
| `ClientOptions.writeTimeoutMs`         | `30,000` ms  | Default per-attempt deadline for `table.append()`, `AppendStream` attempts, and Catalog mutations. |
| `WriteOptions.timeoutMs`               | Inherited    | Per-call override for `table.append()` (`null` disables the SDK deadline for that call).           |
| `CatalogOptions.timeoutMs`             | Inherited    | Per-call override for Catalog `apply()` and `delete()` (`null` disables the SDK deadline).         |
| `AppendStreamBuilder.attemptTimeoutMs` | Inherited    | Per-attempt HTTP deadline for stream batches (`null` disables the per-attempt deadline).           |
| `AppendStreamBuilder.maxElapsedTimeMs` | `300,000` ms | Total per-batch delivery budget across all attempts and retry delays in `AppendStream`.            |
| `RequestOptions.signal`                | `undefined`  | Caller `AbortSignal` supported on all query, statement, catalog, and append operations.            |

A timeout or in-flight cancellation does not roll back a write that has already been sent: `AppendRowsError` reports `outcome: "ambiguous"` with `writeDetails: { reason: "timeout" | "cancelled", outcome: "ambiguous" }`, and Catalog mutations report `catalogOutcome: "unknown"` with `writeDetails`. Statement requests do not use `writeTimeoutMs`; bound them with `.withMaxElapsedSeconds()` on the server and an `AbortSignal` locally.

## Errors

SDK operation errors use `ScopeDBError`, with `AppendRowsError` and `AppendRetryExhaustedError` for Append delivery failures. Caller abort reasons and errors thrown by an input iterable can propagate unchanged, so catch values remain `unknown`:

```ts
import { ScopeDBError } from "scopedb";

try {
  await client.query("SELECT 1");
} catch (error) {
  if (error instanceof ScopeDBError) {
    console.error({
      kind: error.kind,
      code: error.code,
      message: error.message,
      httpStatus: error.httpStatus,
      requestId: error.requestId,
      statementId: error.statementId,
      catalogOutcome: error.catalogOutcome,
      retryable: error.retryable,
      retryAfterMs: error.retryAfterMs,
    });
  }
  throw error;
}
```

Use the operation-specific recovery fields before retrying a failed call:

| Operation      | What to inspect on `ScopeDBError`                                         | Recovery contract                                                                                                                                            |
| -------------- | ------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Queries        | `error.statementId`, `error.retryable`, `error.statementDetails`          | If `error.statementId` is present after a polling failure, resume waiting with `client.statementHandle(error.statementId).wait()` instead of re-running SQL. |
| Table Appends  | `error instanceof AppendRowsError`, `error.outcome`, `error.writeDetails` | `"failed"` confirms the failed request committed no rows; other stream batches may have committed. `"ambiguous"` requires reconciliation before replay.      |
| Catalog Writes | `error.catalogOutcome`, `error.httpStatus`, `error.code`                  | `"rejected"` guarantees the change did not apply (for example, `412 precondition_failed`); `"unknown"` requires calling `get()` to inspect current state.    |

`retryable` describes the error's retry classification; it is not permission to replay an entire workflow. Use the operation-specific fields above. For diagnostics, record `kind`, `code`, `requestId`, and `statementId` when available. An error is not guaranteed to have an HTTP status: encoding, cancellation, and transport failures can occur without a server response.

## CommonJS applications

The package is ESM-only. Load it from CommonJS modules with dynamic `import()`:

```js
async function main() {
  const { Client } = await import("scopedb");
  // ...
}

void main();
```
