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

The SDK compresses JSON request bodies and table append requests with gzip by default using the Web Compression API. Append limits are based on the uncompressed NDJSON body.

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

### Append an NDJSON string

If you already have newline-delimited JSON, pass it to `table.append()`:

```ts
const result = await table.append('{"id":6,"name":"sixth"}\n');
console.log(result.num_rows_inserted);
```

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

Append accepts rows matching the destination columns, not a ScopeQL statement. Move field renaming, type conversion, filtering, and routing into the producer before calling `send()`. For a small producer-side mapping:

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
