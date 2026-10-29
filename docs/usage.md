# Using the JavaScript SDK

Start with the [runnable quickstart](../README.md#run-a-query). The snippets below use its `client`; Append snippets use an existing `events` table and the `table` created in [Append rows](#append-rows). For complete programs, see the [examples](../examples/README.md).

## Track or cancel a query

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

`snapshot()` and `refresh()` return caller-owned copies. A local `AbortSignal` interrupts waiting; use `cancel()` to request server cancellation. `cancel()` returns the complete observed state, which may still be pending/running or may already be finished. `wait()` reports confirmed cancellation with `ScopeDBError.kind === "StatementCancelled"`.

Use `.withMaxElapsedSeconds(60)` to limit elapsed execution time, including queueing, and `.withMaxEstimatedSeconds(5)` to reject a statement whose estimated duration exceeds the limit. These limits take positive seconds. Query waiting has no additional SDK deadline unless the caller supplies a signal.

## Read query results

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

The option only affects `int` / `uint` columns; other types are unchanged. Typed conversions return `timestamp` cells as JavaScript `Date` values and therefore retain only millisecond precision, even when the server returns nanoseconds. Use `rawRows()` when exact timestamp text matters; `integerMode` does not change timestamp conversion.

`first()` returns the first object or `null` for an empty result. Object conversion rejects duplicate column names; use unique aliases or `toValues()` when names repeat. Invalid or out-of-range result cells include row and column context in the error.

## Read or change table schemas

Catalog reads return complete declarative definitions and opaque ETags. Select the smallest subtree you need. Apply replaces that whole subtree, including its children; omitted fields are cleared. Use a dry run and an ETag precondition to review changes and reject concurrent updates.

```ts
const resource = client.table("events").catalog();
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

## Append rows

Use an append stream to write JavaScript objects to an existing table. The SDK groups them into batches automatically. This example uses a table named `events` with `id int` and `name string` columns:

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

// Optional: wait for the current writes while keeping the stream open.
// await stream.flush();

// When finished, wait for remaining writes and close the stream.
await stream.shutdown();
```

`send()` and `sendAll()` add records to the SDK's pending writes. Call `shutdown()` when you are done sending to wait for writing to finish. `sendAll()` accepts an iterable or async iterable. Use `flush()` when you need to wait for the current writes and then continue using the stream.

`sendAll()` is not a transaction over its input: if encoding, iteration, or admission fails midway, earlier rows can already be buffered or committed. Settle the accepted prefix even after an exception; do not replay the entire input just because `sendAll()` threw. The [admission recovery example](../examples/patterns/admission-recovery.ts) keeps a finite source, confirms the prefix, corrects a local encoding error, and submits only the unadmitted suffix. It stops on delivery errors rather than guessing which rows reached the server.

Streams target 4 MiB batches by default, with limits of 8 MiB of uncompressed NDJSON and 200,000 rows per request. Use `targetBatchBytes()` and `maxBatchRows()` to reduce a batch's size. The default failure policy stops the stream after a failed batch; use [continue mode](#continue-after-a-failed-batch) for best-effort sinks.

In stop mode, `flush()` and `shutdown()` return `AppendRowsResult | null`, with `num_rows_inserted` for committed rows and `null` for an empty barrier. In continue mode, they return an `AppendDeliveryReport`; inspect `outcome`, `committedRows`, `failedRows`, and `unknownRows`. Each report covers activity since the previous returned report.

Streams retry confirmed transient rejections by default. Unknown outcomes are not replayed unless `.retryUnknown(true)` is enabled, which can duplicate rows. `.maxRetries(0)` disables retries. Direct `append()` makes one attempt. See [timeouts and HTTP options](#timeouts-and-http-options) for deadlines and interruption outcomes.

### JSON encoding for Append

Each nonblank NDJSON line must be a JSON object keyed by destination column name. Append does not run a ScopeQL projection or general-purpose casts. Use these encodings for non-null input values:

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

The signed 64-bit integer-token limit also applies inside `object`, `array`, and `any`. By default, `appendStream()` uses JavaScript's `JSON.stringify`: unsafe integers may already be rounded before the SDK sees them, `bigint` throws, NaN/Infinity become `null`, and `undefined` object properties are omitted. Opt into [strict encoding](#strict-object-encoding) to preserve int64 `bigint` values and reject lossy numbers before admission. If retaining the default encoder, validate numbers with `Number.isFinite` and use `Number.isSafeInteger` for integer-valued JavaScript numbers. Preserve original NDJSON text when importing data that must not pass through JavaScript number conversion, as shown in [import-ndjson.ts](../examples/import-ndjson.ts). Do not turn integers into quoted numeric strings for an integer column.

For a JavaScript `Date` within the timestamp range above, `toISOString()` produces an accepted UTC timestamp. It has millisecond precision; retain an already valid RFC 3339 source string when sub-millisecond precision matters. Parse and validate any source text that contains serialized objects before passing the resulting object to Append.

### Strict object encoding

Enable `.strictEncoding(true)` before building a stream, or call `encodeAppendRecord()` to prepare one JSON object for direct Append. Both use the same encoder; the default stream encoder remains unchanged.

```ts
import { encodeAppendRecord } from "scopedb";

const stream = table.appendStream().strictEncoding(true).build();
await stream.send({ id: 9007199254740993n, name: "exact integer" });
await stream.shutdown();

// No trailing newline is added; join multiple encoded records with "\n".
await table.append(encodeAppendRecord({ id: 9007199254740995n, name: "direct" }));
```

Strict encoding accepts plain objects (including null-prototype objects), arrays, strings, booleans, null, valid Dates, finite numbers, and signed 64-bit bigints. Bigints become exact unquoted decimal tokens, including inside objects and arrays. Integer-valued numbers must satisfy `Number.isSafeInteger`; this conservative rule also applies when the intended column is a float. An already rounded Number cannot be repaired: construct a bigint from the original decimal text or retain validated NDJSON instead. The server's signed 64-bit input ceiling still applies to uint columns; this encoder does not extend it.

Undefined values, functions, symbols, sparse array entries, invalid Dates, cycles, accessor properties, and custom objects such as Map or class instances fail locally instead of being omitted or substituted. Plain objects use own enumerable string data properties; enumerable symbol keys are rejected. Convert custom objects explicitly, omit absent fields intentionally, or supply null. Custom `toJSON()` hooks are not called. A nested Date is encoded using its ISO timestamp with millisecond precision; preserve a valid timestamp string when nanoseconds matter. Query results with JSON-encoded object/array cells still require explicit parsing before writing to those column types.

Value validation errors are permanent `ScopeDBError` values with `kind: "AppendRowsFailed"` and a field path in the message and `error.context().get("record_path")`, for example `$["attrs"][0]["score"]`. `send()`/`sendAll()` reject before admitting that record; previously admitted records remain pending. `trySend()` returns false and increments `droppedByReason.invalidRecord`. The helper validates values, not table schemas or request limits: quoted numeric strings remain strings, and destination type errors can still reject an entire append request.

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

`trySend()` returns false for local admission failures and records the reason in `stats()`. Continue mode does not retain failed payloads for later replay. Its circuit breaker opens after consecutive availability failures, rejects `trySend()` admission during cooldown, and allows one `trySend()` probe afterwards; `send()` and `sendAll()` can still fill the bounded local queue while dispatch is paused. A permanent schema error does not open the breaker. The default breaker settings are five failures and a 30-second cooldown. Use a durable outbox when losing telemetry is unacceptable. See the runnable [telemetry example](../examples/patterns/telemetry.ts).

| Metric                   | `stats()` field                                                  | Interpretation                                                                                                                                                                                                                                                                               |
| ------------------------ | ---------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Dropped before admission | `droppedRows`, `droppedByReason`                                 | Failed `trySend()` calls, split into buffer full, invalid record, oversized record, open circuit, and closed stream. Thrown `send()`/`sendAll()` admission errors must be counted by the caller.                                                                                             |
| Known not committed      | `failedRows`                                                     | Admitted rows whose batch ended in rejection; also includes admitted but unsent rows discarded after a fatal stream failure. For specifically server-rejected batches, count `batchRows` in `onBatchFailure` when `outcome === "rejected"`, using HTTP metadata to distinguish local errors. |
| Possibly committed       | `unknownRows`                                                    | Admitted rows with an unresolved commit outcome; keep separate from definite loss and never blindly replay them.                                                                                                                                                                             |
| Confirmed committed      | `committedRows`                                                  | Rows acknowledged as committed.                                                                                                                                                                                                                                                              |
| Retry pressure           | `retries`, `lastFailure.error`                                   | Extra attempts and complete final failure diagnostics, including HTTP status and `retryAfterMs`.                                                                                                                                                                                             |
| Current backlog          | `pendingRows`, `pendingBytes`, `inFlightBatches`, `circuitState` | Gauges for backpressure, requests in flight, and circuit state.                                                                                                                                                                                                                              |

Row/retry counters are cumulative for the stream's lifetime and are not reset by `flush()`. Export their deltas or maintain a baseline per stream. `flush()`/`shutdown()` delivery reports cover activity since the previous returned report; do not add them to the lifetime counters again. Aborting `flush()` stops only local waiting, not delivery; its outcome is merged into the next `flush()` or `shutdown()` report so unresolved rows are not hidden.

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

An unknown result, including `writeDetails.outcome === "unknown"` after timeout or cancellation, may already be committed: stop and reconcile it. A subsequent definite rejection cannot resolve an earlier unknown attempt. Avoid layering an application retry loop around a retrying stream. A type-checked copy is in [retry-append.ts](../examples/templates/retry-append.ts).

## Timeouts and HTTP options

Each append attempt and Catalog mutation defaults to a 30-second SDK deadline. Its budget starts after local encoding, compression, and request preparation and includes reading the complete response. `ClientOptions.writeTimeoutMs` sets the default; direct writes and Catalog mutations accept `timeoutMs`, and streams accept `.attemptTimeoutMs()`. An omitted setting inherits the default; a positive integer overrides it; `null` disables that SDK deadline. Zero is invalid. Caller `AbortSignal` and transport deadlines remain effective. The stream's separate `.maxElapsedTimeMs()` delivery budget defaults to five minutes per batch and includes all attempts and retry delays, even when its per-attempt deadline is disabled.

A timeout or in-flight cancellation does not roll back a write. Append errors retain their cause and HTTP diagnostics, with `writeDetails: { reason: "timeout" | "cancelled", outcome: "unknown" }`; Catalog mutations report `code: "catalog_commit_outcome_unknown"`. Cancellation before sending preserves the caller's abort reason. Streams never replay client timeouts or cancellations, including when unknown replay is enabled.

Streams retry confirmed transient rejections by default. An unknown outcome stops that batch without replay because the original attempt may have committed. Set `.retryUnknown(true)` only when the application accepts duplicates or can reconcile them; explicit server retry refusal is always honored. `.maxRetries(0)` disables all retries. Direct `append()` and Catalog mutations make one attempt.

HTTP redirects retain the fetch transport's default behavior. A custom `ClientOptions.fetch` can implement an application-specific redirect policy. Redirects are distinct from the SDK's append retry policy and share the attempt deadline.

The SDK compresses Statement and Append request bodies with gzip using the Web Compression API. Catalog mutations use plain JSON. Append limits are based on the uncompressed NDJSON body.

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
      statementId: error.statementId,
      retryable: error.retryable,
      retryAfterMs: error.retryAfterMs,
      statementDetails: error.statementDetails,
    });
  }
  throw error;
}
```

`retryable` does not prove that replay is safe. After a successful submission, errors from `query()` and `execute()` are not marked retryable: repeating those methods would submit a second execution. Their SDK errors retain `statementId`, along with the original cause and server diagnostics. After deciding a polling failure is recoverable, use `client.statementHandle(error.statementId).wait({ signal })` with an active signal to resume the same execution. If you already retained a handle, retry waiting on that handle; its `refresh()` and `wait()` errors keep their temporary classification. The SDK does not retry failed polling requests automatically.

A lost transport acknowledgement or an unstructured 408/429/5xx response during Statement submission has code `statement_submission_outcome_unknown` and is not marked retryable. Supply a caller-known UUID with `withStatementId()` before submission to retain that ID on SDK errors even if the response is lost. An error's `statementId` identifies the attempted statement; it does not prove acceptance or guarantee that the server still retains its result. Confirmed temporary submission rejections keep their retryable classification. Append and Catalog follow the outcome rules above.

Caller abort reasons are rethrown unchanged, without adding a statement ID or changing their retryability. Retain the handle or choose an ID before submission when cancellation recovery is needed.

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
