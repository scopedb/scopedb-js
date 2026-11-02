# Upgrade from 0.3 to 0.4

Version 0.4 changes object creation, Catalog version preconditions, and Append outcome names, and adds strict record encoding. Applications upgrading from 0.2 should first follow [Upgrade from 0.2 to 0.3](migration-0.3.md).

## Object creation

Create tables, statements, handles, and catalog resources through a `Client`. Their constructors are now private in TypeScript:

| In 0.3                                 | In 0.4                                |
| -------------------------------------- | ------------------------------------- |
| `new Table(client, name, options)`     | `client.table(name, options)`         |
| `new Statement(client, sql)`           | `client.statement(sql)`               |
| `new StatementHandle(client, id)`      | `client.statementHandle(id)`          |
| `new Catalog(transport)`               | `client.catalog()`                    |
| `new CatalogResource(transport, path)` | `client.catalog()` resource selectors |

Select a catalog resource with `client.catalog().database(name)`, `.schema(database, name)`, `.table(database, schema, name)`, or `.view(database, schema, name)`.

To reattach to an existing statement, use `client.statementHandle(id)` and call `refresh()` or `wait()` to retrieve its state. Supplying an initial snapshot to a handle constructor is no longer supported. If you previously supplied a custom Catalog transport, configure HTTP behavior through the `Client`'s `fetch` option.

`ResultSet.fromStatementResultSet()` is no longer part of the public TypeScript API. Obtain query results through `client.query()` or `handle.wait()`. To construct a result yourself, such as an application test fixture, use the public result constructors:

```ts
import { FieldSchema, ResultSet, Schema } from "scopedb";

const result = new ResultSet(
  new Schema([new FieldSchema("id", "int")]),
  1,
  [["42"]],
);
```

## Catalog versions and mutation outcomes

Replace `snapshot.etag` and `result.etag` with `.version`, and replace the `ifMatch` option with `expectedVersion`:

| In 0.3                                      | In 0.4                                                    |
| ------------------------------------------- | --------------------------------------------------------- |
| `CatalogSnapshot.etag`                      | `CatalogSnapshot.version`                                 |
| `CatalogResult.etag`                        | `CatalogResult.version`                                   |
| `CatalogOptions.ifMatch`                    | `CatalogOptions.expectedVersion`                          |
| `catalog_commit_outcome_unknown` code check | `ScopeDBError.catalogOutcome` (`"rejected" \| "unknown"`) |

```ts
const resource = client.table("events").catalog();
const snapshot = await resource.get();
const definition = { ...snapshot.definition, comment: "Updated events" };
const preview = await resource.apply(definition, {
  dryRun: true,
  expectedVersion: snapshot.version,
});
await resource.apply(definition, { expectedVersion: preview.version });
```

Pass `version` unchanged (including its quotes) as `expectedVersion`. The version token covers the tenant catalog; a concurrent catalog change fails atomically with HTTP `412` (`code: "precondition_failed"`). When a catalog mutation (`apply()` or `delete()`) fails, inspect `error.catalogOutcome`: `"rejected"` confirms the change did not take effect, whereas `"unknown"` means the mutation may have committed and you should call `get()` before retrying.

## Append outcomes and replay

Update Append outcome types, builder methods, and delivery report fields:

| In 0.3                            | In 0.4                              |
| --------------------------------- | ----------------------------------- |
| `AppendState`                     | `AppendOutcome`                     |
| `status: "committed"`             | `status: "succeeded"`               |
| `error.outcome === "rejected"`    | `error.outcome === "failed"`        |
| `error.outcome === "unknown"`     | `error.outcome === "ambiguous"`     |
| `writeDetails.outcome: "unknown"` | `writeDetails.outcome: "ambiguous"` |
| `.retryUnknown(true)`             | `.retryAmbiguous(true)`             |
| `unknownRows`, `unknownBatches`   | `ambiguousRows`, `ambiguousBatches` |
| Report `"ok"`, `"unknown"`        | Report `"succeeded"`, `"ambiguous"` |

These are runtime string changes, not just TypeScript type renames. Update string comparisons in error classifiers and retry logic, including code that reads errors as untyped objects. Update mocked errors and serialized fixtures as well: old `"unknown"` / `"rejected"` fixtures can keep tests passing while the classifier mishandles real 0.4 errors. Add consumer tests using 0.4 error shapes for confirmed temporary rejection, ambiguous delivery, and write interruption (`writeDetails.outcome: "ambiguous"`). Changing only the dependency version is insufficient.

Limit these replacements to Append outcomes and `writeDetails.outcome`. `catalogOutcome` still uses `"rejected"` / `"unknown"`; unrelated ETags, such as object-storage ETags, are unaffected.

- `error.outcome === "failed"` guarantees that no rows in the batch were committed.
- `error.outcome === "ambiguous"` means the batch may have committed (for example, on HTTP `5xx`, network disconnects, or timeouts).
- By default, `AppendStream` automatically retries only confirmed `HTTP 429` admission refusals (`outcome: "failed"`). Enable `.retryAmbiguous(true)` only when your pipeline can tolerate or deduplicate replayed rows. Client timeouts and cancellations are never replayed automatically.

Caller-managed retries must preserve the same distinction. An HTTP `503` alone does not prove that a batch was rejected: requeueing an ambiguous batch can duplicate committed rows. Reconcile its outcome before replaying, or use a delivery policy that explicitly accepts duplicate rows or deduplicates writes.

## Strict record encoding and statement recovery

- Use `encodeAppendRecord(record)` or `table.appendStream().strictEncoding(true)` to serialize `bigint` values losslessly as 64-bit JSON integer tokens and reject `NaN`, `Infinity`, unsafe integer `number`s, `undefined`, custom class instances, and circular references locally before buffering. See [Strict object encoding](usage.md#strict-object-encoding).
- When `client.query()` or `statement.execute()` fails after submission (for example, due to a transient polling failure), the thrown `ScopeDBError` preserves `error.statementId` and is marked non-retryable so callers resume waiting with `client.statementHandle(error.statementId).wait()` rather than submitting the SQL a second time.
