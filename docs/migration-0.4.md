# Upgrade from 0.3 to 0.4

The next minor release aligns Catalog concurrency and Append delivery with the current API contract. These changes are currently unreleased; update application code when upgrading to 0.4.

## Catalog versions

Replace `snapshot.etag` and `result.etag` with `.version`, and replace the `ifMatch` option with `expectedVersion`:

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

The opaque token represents the whole tenant catalog, even when reading one resource. Preserve it exactly, including quotes. A catalog change elsewhere in the tenant can invalidate it; data appends do not advance it. Dry runs return the base version, writes return the resulting version, and no-op writes retain the version. The SDK carries the token through the HTTP `ETag` and `If-Match` headers. Stale versions fail atomically with HTTP 412 and `precondition_failed`.

Use `ScopeDBError.catalogOutcome` instead of checking for an SDK-generated `catalog_commit_outcome_unknown` code. `"rejected"` means the mutation was rejected; `"unknown"` means it may have committed, including coded gateway failures, timeouts, transport failures, and invalid acknowledgements. Inspect the original server `code`, `details`, and HTTP metadata separately. Read the current definition before deciding whether to replay. Reads, dry runs, and local validation failures have no mutation outcome.

## Append outcomes and replay

| Before                          | After                               |
| ------------------------------- | ----------------------------------- |
| `AppendState`                   | `AppendOutcome`                     |
| `status: "committed"`           | `status: "succeeded"`               |
| `error.outcome === "rejected"`  | `error.outcome === "failed"`        |
| `error.outcome === "unknown"`   | `error.outcome === "ambiguous"`     |
| `.retryUnknown(true)`           | `.retryAmbiguous(true)`             |
| `unknownRows`, `unknownBatches` | `ambiguousRows`, `ambiguousBatches` |
| Report `"ok"`, `"unknown"`      | Report `"succeeded"`, `"ambiguous"` |

`writeDetails.outcome` and batch-failure callbacks also use the new outcome values. Update persisted SDK-result consumers and telemetry filters accordingly. Counts of confirmed rows remain `committedRows`; row-count acknowledgements remain `num_rows_inserted`.

HTTP 200 with a matching `num_rows_inserted` confirms success without requiring a wire `status` field. Legacy `status: "committed"` acknowledgements are still accepted. Valid nested HTTP 4xx errors establish failure; HTTP 5xx errors remain ambiguous. Known codes must match their HTTP status; unknown codes are retained. Missing or malformed error bodies, inconsistent codes, invalid acknowledgements, and row-count mismatches are ambiguous. Optional legacy row diagnostics remain available, but modern errors describe the entire request; do not parse messages for per-row outcomes.

Streams automatically retry only valid HTTP 429 rejections. A valid HTTP 408 failure is not automatically retried, even with a positive server retry hint. A bodyless or malformed 429 is ambiguous. `.retryAmbiguous(true)` permits bounded replay of transient ambiguous failures and may duplicate rows; client timeouts, cancellations, and explicit server retry refusal still stop replay. A later definite failure cannot resolve an earlier ambiguous attempt. Direct `append()` makes one attempt.
