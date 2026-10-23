# Upgrade from 0.2 to 0.3

Update your dependency with `npm install scopedb@0.3`, then follow the sections for APIs your application uses. Node.js 20 remains the minimum supported version, unchanged from 0.2. The package remains ESM-only.

## Replace Ingest with Append

| In 0.2                                 | In 0.3                                  |
| -------------------------------------- | --------------------------------------- |
| `Client.insert()` / `Client.ingest()`  | `client.table(name).append(ndjson)`     |
| `Client.ingestStream()`                | `client.table(name).appendStream()`     |
| `IngestStream` / `IngestStreamBuilder` | `AppendStream` / `AppendStreamBuilder`  |

Append writes rows whose fields match an existing destination table. It does not take a ScopeQL transform. Move field renaming, conversion, filtering, and routing into your application before sending. For an `events` table with `occurred_at timestamp` and `name string` columns:

```ts
const source = { ts: "2026-03-13T12:00:00Z", name: "page_view" };
const stream = client.table("events").appendStream().build();
await stream.send({
  occurred_at: new Date(source.ts).toISOString(),
  name: source.name,
});
const result = await stream.shutdown();
console.log("Written rows:", result?.num_rows_inserted ?? 0);
```

The snippets in this guide use the `client` from the [quickstart](../README.md#run-a-query). `Date.toISOString()` retains milliseconds; keep an already valid RFC 3339 source string when sub-millisecond precision matters. Follow the [Append JSON encoding rules](usage.md#json-encoding-for-append), including timestamp formats, objects/arrays, missing columns, and exact integer input.

For a transformation that needs ScopeQL, append raw events to an `object` column in a staging table, then run a separate `FROM ... SELECT ... INSERT INTO ...` statement. These are two writes: track which batches were transformed before retrying.

When replacing an Ingest stream:

- Direct `append()` returns `AppendRowsResult`. In the default stop policy, `flush()` and `shutdown()` return `AppendRowsResult | null`; read `num_rows_inserted`, with `null` for an empty barrier. Continue mode returns `AppendDeliveryReport`; inspect `outcome`, `committedRows`, `failedRows`, and `unknownRows`.
- `send()` and `sendAll()` queue rows. Await `flush()` to settle admitted writes while keeping the stream open, or `shutdown()` when finished. Each returned report covers activity since the previous returned report.
- Lower batch sizes above 8 MiB. Append requests allow at most 8 MiB of uncompressed NDJSON and 200,000 rows; the default byte target is 4 MiB.
- Set `.maxConcurrentBatches(1)` if you need sequential requests. The default is four concurrent batches per stream; query result order still requires `ORDER BY`.

Streams retry confirmed temporary rejections by default. As in 0.2, unknown outcomes are not replayed by default; 0.3 adds `.retryUnknown(true)` for applications that can accept or reconcile duplicates. Client timeouts and cancellations are never replayed. `.maxRetries(0)` disables retries, and direct `append()` makes one attempt. For long-lived sinks, use the [continue-mode telemetry recipe](usage.md#long-lived-telemetry-and-rate-limiting).

## Update Catalog reads and mutations

Replace paginated `list*`, `fetch*`, and `iterate*` calls with a resource selector followed by `get()`. Each read returns a complete `definition` and an opaque `etag`. In the table below, `catalog` is `client.catalog()`.

| In 0.2                                           | In 0.3                                     |
| ------------------------------------------------ | ------------------------------------------ |
| List databases                                   | `catalog.get()`                            |
| List schemas in a database                       | `catalog.database(db).get()`               |
| List tables in a schema                          | `catalog.schema(db, schema).get()`         |
| Fetch a table / `describe()` / `tableSchema()`   | `table.catalog().get()`                    |
| `table.apply()`                                  | `table.catalog().apply(definition)`        |
| `table.drop()`                                   | `table.catalog().delete(options)`          |
| `distinctOn` / `distinct_on`                     | `definition.distinct` (with `on` and `by`) |
| `dataRetentionDays` / `data_retention_days`      | `definition.retention_days`                |
| `partitionBy` / `clusterBy`                      | `definition.partition_by` / `cluster_by`   |
| Column `dataType`                                | Column `data_type`                         |

For example, list the tables in a schema:

```ts
const snapshot = await client.catalog().schema("scopedb", "public").get();
for (const table of snapshot.definition.tables ?? []) {
  console.log(table.name, table.columns?.length ?? 0);
}
```

Definitions now use snake_case fields and preserve extension fields. An `apply()` replaces the entire selected resource, including children; omitted fields are cleared. For updates, read the definition, change it, and carry its ETag through a dry-run and the mutation. Potentially destructive changes require `allowDestructive: true`. See the [Catalog example](usage.md#read-or-change-table-schemas).

## Update statements and query limits

| In 0.2                                      | In 0.3                                |
| ------------------------------------------- | ------------------------------------- |
| `withExecTimeout("30s")`                    | `withMaxElapsedSeconds(30)`           |
| `withMaxParallelism(...)`                   | Removed                               |
| `handle.lastStatus()`                       | `handle.snapshot()`                   |
| `handle.status()` / `fetchOnce()`           | `handle.refresh()`                    |
| `handle.fetch()`                            | `handle.wait()`                       |
| Handle `progress()` / `resultSet()`         | Inspect `snapshot()` or call `wait()` |
| `StatementStatus*`                          | `StatementSnapshot*`                  |
| `FetchOptions`                              | `WaitOptions`                         |

Execution limits now use positive seconds instead of duration strings. `withMaxElapsedSeconds()` includes queueing time. The separate `withMaxEstimatedSeconds()` option rejects queries whose estimated duration exceeds a limit; it does not control parallelism. Omitted limits use service defaults.

```ts
const handle = await client.statement("SELECT 1 AS ready")
  .withMaxElapsedSeconds(60)
  .submit();
const result = await handle.wait();
console.log(result.toObjects());
```

`snapshot()` is synchronous and local; `refresh()` observes the server while the statement is active. Both return caller-owned copies. Concurrent observations cannot replace a cached terminal result with an older state.

`cancel()` now returns a complete snapshot. A query can finish before cancellation takes effect; pending or running states still need `wait()`. Confirmed cancellation makes `wait()` throw a `ScopeDBError` with `kind === "StatementCancelled"`. Aborting a local signal only stops waiting.

When matching snapshots directly, read `failed.error` instead of `failed.message`. Cancelled snapshots no longer carry a message. Keep the handle when recovering from a failed status read; do not resubmit the whole statement. For a submission whose response might be lost, supply a known UUID with `withStatementId()` and reconcile through `client.statementHandle(id)`.

## Review write deadlines and delivery reports

Appends and Catalog mutations now default to a 30-second timeout per HTTP attempt, through reading the response body. Override it with `ClientOptions.writeTimeoutMs`, per-request `timeoutMs`, or a stream's `.attemptTimeoutMs()`. An omitted option inherits the default; a positive integer changes it; `null` disables that attempt deadline. Zero is invalid.

Streams also have a five-minute per-batch budget covering requests and retry delays. Configure it with `.maxElapsedTimeMs()`. Disabling the attempt deadline does not disable this budget or caller cancellation. Query waiting has no additional SDK deadline unless you supply a signal; service execution limits still apply.

A timeout does not roll back a write. Inspect Append `outcome` and `writeDetails`, or Catalog's `catalog_commit_outcome_unknown` error code, before deciding whether to retry. A temporary error alone does not make replay safe. See [caller-managed retries](usage.md#caller-managed-retries).

Aborting `flush()` only stops local waiting. Its delivery outcomes are included in the next `flush()` or `shutdown()` report, without double counting. `stats()` counters cover the stream's lifetime and are not reset by a flush. Use the [telemetry metrics](usage.md#long-lived-telemetry-and-rate-limiting) to keep dropped, rejected, and unknown outcomes separate.

## Rename remaining fields and methods

| In 0.2                                                                  | In 0.3                                                      |
| ----------------------------------------------------------------------- | ----------------------------------------------------------- |
| `ClientOptions.token`                                                   | `apiKey`                                                    |
| `table.withDatabase()` / `withSchema()`                                 | `client.table(name, { database, schema })`                  |
| `jsonRows()` / `intoValues()` / `intoObjects()`                         | `rawRows()` / `toValues()` / `toObjects()`                  |
| `IntoOptions`                                                           | `ResultOptions`                                             |
| `executeOne()`                                                          | `(await statement.execute()).first()`                       |
| `batchBytes` / `flushInterval`                                          | `targetBatchBytes` / `flushIntervalMs`                      |
| `maxPendingBytes` / `maxInFlightRequests`                               | `maxBufferedBytes` / `maxConcurrentBatches`                 |
| `attemptTimeout`                                                        | `attemptTimeoutMs`                                          |
| Append result `append_state`                                            | `status`                                                    |
| `AppendRowsError.appendState`                                           | `outcome`                                                   |
| Separate `stats().lastFailure` message/status fields                    | `stats().lastFailure.error`                                 |
| `u_int`                                                                 | `uint`                                                      |
| Low-level `Client` transport methods                                    | `Table.append()`, `Statement.submit()`, and handle methods  |

`Table.identifier()` now always includes database and schema, defaulting to `scopedb` and `public`. Integer results still default to exact `bigint`; use `integerMode: "string"` for JSON output. Preserve original NDJSON text when importing integers that JavaScript numbers cannot represent exactly.
