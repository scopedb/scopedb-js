# Using the JavaScript SDK

Start with the [runnable quickstart](../README.md#run-a-query). The snippets below use its `client`; Append snippets use an existing `events` table. For complete programs, see the [examples](../examples/README.md).

| Component                       | Purpose                                                                                             |
| ------------------------------- | --------------------------------------------------------------------------------------------------- |
| `Client`                        | Entry point configured with your ScopeDB endpoint, API key, default write timeout, and `fetch`.     |
| `Statement` / `StatementHandle` | Submits ScopeQL queries, configures execution limits, and tracks, cancels, or resumes execution.    |
| `ResultSet`                     | Decodes tabular results into row arrays or column-keyed objects with configurable integer modes.    |
| `Table` / `AppendStream`        | Appends rows to a table via managed background batching (`appendStream()`) or one-shot NDJSON.      |
| `Catalog` / `CatalogResource`   | Reads and updates declarative database, schema, table, and view definitions with version checks.    |
| `ScopeDBError`                  | Structured error type exposing failure categories, retryability, and write/statement recovery data. |

## Read query results

`client.query(scopeql)` submits a statement, waits for completion, and returns a `ResultSet`. By default, `int` and `uint` columns decode to JavaScript `bigint` so signed and unsigned 64-bit values never lose precision. Because `JSON.stringify()` does not serialize `bigint` values directly, `toValues()`, `toObjects()`, and `first()` accept an `{ integerMode }` option:

| `integerMode`        | Return type | When to use                                                                                                        |
| -------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------ |
| `"bigint"` (default) | `bigint`    | General application logic requiring exact 64-bit integers.                                                         |
| `"number"`           | `number`    | JSON responses and bounded counters within `Number.MIN_SAFE_INTEGER`..`Number.MAX_SAFE_INTEGER` ($\pm(2^{53}-1)$). |
| `"string"`           | `string`    | JSON responses containing unbounded 64-bit identifiers or counters without precision loss.                         |

```ts
// Default: lossless bigint
const rowsBigint = result.toObjects();

// JSON-safe number for bounded counts and small integers
const rowsNumber = result.toObjects({ integerMode: "number" });
JSON.stringify(rowsNumber);

// Lossless decimal string for JSON payloads with 64-bit IDs
const rowsString = result.toObjects({ integerMode: "string" });
```

`integerMode` affects only `int` and `uint` columns. `timestamp` columns decode to `Date` (millisecond precision); use `rawRows()` when you need the raw nanosecond timestamp text returned by the server. `first()` returns the first row object or `null` when the result is empty. `toObjects()` and `first()` reject duplicate column names; use unique column aliases in SQL or read positional cells with `toValues()`.

## Track or cancel a query

For long-running or non-idempotent statements, build a `Statement` with `client.statement(scopeql)` to configure execution limits or manage the execution handle directly:

```ts
const handle = await client.statement("SELECT 1 AS ready")
  .withMaxElapsedSeconds(60)
  .withMaxEstimatedSeconds(5)
  .submit();

// Synchronous local observation from submit(), refresh(), wait(), or cancel()
console.log(handle.snapshot()?.status);

// One remote status poll while the statement is pending or running
const latest = await handle.refresh();
console.log(latest.status);

// Poll until finished, failed, or cancelled and decode the ResultSet
const result = await handle.wait();
```

- `withMaxElapsedSeconds(seconds)` bounds total execution time including queueing; `withMaxEstimatedSeconds(seconds)` rejects queries whose estimated runtime exceeds the threshold.
- `refresh()` and `cancel()` return the cached snapshot without making a network request once the handle reaches a terminal state (`finished`, `failed`, or `cancelled`).
- Passing an `AbortSignal` to `wait()`, `query()`, or `execute()` stops local waiting without cancelling the server statement; call `handle.cancel()` to request server-side cancellation.
- Use `client.statementHandle(statementId)` to reattach to a previously submitted statement by its UUID.

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

## Append rows

ScopeDB provides two table write paths on `client.table(name, { database, schema })`:

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

| Builder method                  | Default          | Description                                                                                         |
| ------------------------------- | ---------------- | --------------------------------------------------------------------------------------------------- |
| `.strictEncoding(enabled)`      | `false`          | Use `encodeAppendRecord()` to support `bigint` and reject lossy numbers or unsupported types.       |
| `.targetBatchBytes(bytes)`      | `4 MiB`          | Target uncompressed NDJSON batch size before sealing a batch (maximum `8 MiB`).                     |
| `.maxBatchRows(rows)`           | `200,000`        | Maximum rows per append request (maximum `200,000`).                                                |
| `.flushIntervalMs(ms)`          | `1,000`          | Maximum delay from the first buffered row in a batch until dispatch.                                |
| `.maxBufferedBytes(bytes)`      | `64 MiB`         | Total buffered and in-flight byte budget before `send()` applies backpressure or `trySend()` drops. |
| `.maxConcurrentBatches(count)`  | `4`              | Maximum concurrent HTTP append requests per stream instance.                                        |
| `.attemptTimeoutMs(ms \| null)` | `writeTimeoutMs` | Per-attempt HTTP deadline in milliseconds (`null` disables the per-attempt deadline).               |
| `.maxElapsedTimeMs(ms)`         | `300,000`        | Total per-batch delivery budget across attempts and retry backoffs.                                 |
| `.maxRetries(count)`            | `8`              | Maximum retries for confirmed temporary admission refusals (`0` disables retries).                  |
| `.retryAmbiguous(enabled)`      | `false`          | Also retry transient `"ambiguous"` outcomes (can duplicate rows if an earlier attempt committed).   |

In the default `"stop"` policy, `flush()` and `shutdown()` resolve to `AppendRowsResult | null` (`{ status: "succeeded", num_rows_inserted }` when rows were committed in the interval, or `null` when no rows were pending) and reject on any unrecovered batch failure.

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

Strict encoding accepts plain objects, arrays, strings, booleans, `null`, valid `Date` instances (serialized via `toISOString()`), finite numbers (integer-valued `number`s must satisfy `Number.isSafeInteger`), and signed 64-bit `bigint`s. It rejects `NaN`, `Infinity`, unsafe integers, `undefined`, functions, symbols, sparse arrays, custom class instances (`Map`, `Set`, `Uint8Array`), accessor properties, and circular references with a permanent `ScopeDBError` (`kind: "AppendRowsFailed"`) that records the offending field path in `error.context().get("record_path")` (such as `$["attrs"][0]["score"]`).

### Continue after a failed batch

Set `{ failurePolicy: "continue" }` when a stream should keep processing subsequent batches after a batch fails. In continue mode, `flush()` and `shutdown()` resolve to an `AppendDeliveryReport` summarizing the interval since the previous barrier:

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

`table.append()` makes a single HTTP attempt. If you write a custom retry loop around `table.append()`, retry only when `error instanceof AppendRowsError`, `error.outcome === "failed"`, `error.isTemporary()` is `true`, `error.serverRetryable !== false`, and `error.writeDetails === undefined`:

```ts
import { setTimeout as delay } from "node:timers/promises";
import { AppendRowsError, type Table } from "scopedb";

async function appendWithRetry(table: Table, ndjson: string, signal: AbortSignal) {
  for (let retries = 0; ; retries++) {
    signal.throwIfAborted();
    try {
      return await table.append(ndjson, { signal });
    } catch (error) {
      if (
        !(error instanceof AppendRowsError) ||
        error.outcome !== "failed" ||
        !error.isTemporary() ||
        error.serverRetryable === false ||
        error.writeDetails !== undefined ||
        retries >= 2
      ) {
        throw error;
      }
      const backoffMs = 200 * 2 ** retries * (0.5 + Math.random() / 2);
      const waitMs = Math.max(backoffMs, error.retryAfterMs ?? 0);
      if (waitMs > 30_000) throw error;
      await delay(waitMs, undefined, { signal });
    }
  }
}
```

When `error.outcome === "ambiguous"` (for example, after a network disconnect, timeout, or HTTP `5xx`), the write may already have committed; reconcile downstream state before replaying to avoid duplicating rows. See [`examples/templates/retry-append.ts`](../examples/templates/retry-append.ts).

## Timeouts and HTTP options

Configure default write timeouts, custom headers, or a custom `fetch` implementation when constructing `Client`:

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

All SDK failures throw `ScopeDBError` (or subclasses `AppendRowsError` and `AppendRetryExhaustedError`):

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
| Table Appends  | `error instanceof AppendRowsError`, `error.outcome`, `error.writeDetails` | `"failed"` guarantees no rows committed; `"ambiguous"` means rows may have committed and require reconciliation before replay.                               |
| Catalog Writes | `error.catalogOutcome`, `error.httpStatus`, `error.code`                  | `"rejected"` guarantees the change did not apply (for example, `412 precondition_failed`); `"unknown"` requires calling `get()` to inspect current state.    |

To make statement submissions recoverable even if the initial submission response is lost in transit, assign a UUID before submitting with `client.statement(sql).withStatementId(uuid).submit()`; if submission fails ambiguously (`code: "statement_submission_outcome_unknown"`), `error.statementId` retains your UUID so you can inspect it with `client.statementHandle(uuid)`.

## CommonJS applications

The package is ESM-only. Load it from CommonJS modules with dynamic `import()`:

```js
async function main() {
  const { Client } = await import("scopedb");
  // ...
}

void main();
```
