# Upgrade from 0.3 to 0.4

The next minor release aligns Catalog version preconditions, Append outcome names, and strict record encoding with the current SDK contract. Applications upgrading from 0.2 should first follow [Upgrade from 0.2 to 0.3](migration-0.3.md).

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

| In 0.3                          | In 0.4                              |
| ------------------------------- | ----------------------------------- |
| `AppendState`                   | `AppendOutcome`                     |
| `status: "committed"`           | `status: "succeeded"`               |
| `error.outcome === "rejected"`  | `error.outcome === "failed"`        |
| `error.outcome === "unknown"`   | `error.outcome === "ambiguous"`     |
| `.retryUnknown(true)`           | `.retryAmbiguous(true)`             |
| `unknownRows`, `unknownBatches` | `ambiguousRows`, `ambiguousBatches` |
| Report `"ok"`, `"unknown"`      | Report `"succeeded"`, `"ambiguous"` |

- `error.outcome === "failed"` guarantees that no rows in the batch were committed.
- `error.outcome === "ambiguous"` means the batch may have committed (for example, on HTTP `5xx`, network disconnects, or timeouts).
- By default, `AppendStream` automatically retries only confirmed `HTTP 429` admission refusals (`outcome: "failed"`). Enable `.retryAmbiguous(true)` only when your pipeline can tolerate or deduplicate replayed rows. Client timeouts and cancellations are never replayed automatically.

## Strict record encoding and statement recovery

- Use `encodeAppendRecord(record)` or `table.appendStream().strictEncoding(true)` to serialize `bigint` values losslessly as 64-bit JSON integer tokens and reject `NaN`, `Infinity`, unsafe integer `number`s, `undefined`, custom class instances, and circular references locally before buffering. See [Strict object encoding](usage.md#strict-object-encoding).
- When `client.query()` or `statement.execute()` fails after submission (for example, due to a transient polling failure), the thrown `ScopeDBError` preserves `error.statementId` and is marked non-retryable so callers resume waiting with `client.statementHandle(error.statementId).wait()` rather than submitting the SQL a second time.
