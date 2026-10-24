/*
 * Copyright 2024 ScopeDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import assert from "node:assert/strict";
import { it } from "node:test";
import {
  AppendRowsError,
  Client,
  ResultSet,
  ScopeDBError,
} from "../src/index.js";
import { Deferred } from "../src/stream-internals.js";
import {
  cancelledStatus,
  emptyResultSet,
  failedStatus,
  finishedStatus,
  jsonResponse,
  requestBodyText,
  runningStatus,
} from "./helpers.js";

for (
  const terminal of [
    finishedStatus(emptyResultSet()),
    failedStatus("original failure"),
    cancelledStatus(),
  ]
) {
  it(`keeps ${terminal.status} after an older running response and isolates caller mutations`, async () => {
    const started = new Deferred<void>();
    const response = new Deferred<Response>();
    let calls = 0;
    const client = new Client("https://example.com", {
      fetch: async (_url, init) => {
        calls++;
        if (init?.method === "POST") return jsonResponse(200, terminal);
        started.resolve();
        return response.promise;
      },
    });
    const handle = client.statementHandle("stmt-1");
    const refreshing = handle.refresh();
    await started.promise;
    const cancelled = await handle.cancel();
    cancelled.progress.compute_seconds = 999;
    response.resolve(jsonResponse(200, runningStatus()));
    assert.equal((await refreshing).status, terminal.status);
    assert.deepEqual(handle.snapshot(), terminal);
    assert.deepEqual(await handle.refresh(), terminal);
    assert.equal(calls, 2);
    if (terminal.status === "finished") {
      assert.equal((await handle.wait()).numRows(), 0);
    }
  });
}

it("preserves complete failure details after a cancellation still observes running", async () => {
  const failure = {
    ...failedStatus("later failure"),
    error: {
      code: "future_failure",
      message: "later failure",
      details: { phase: "commit" },
    },
  };
  const client = new Client("https://example.com", {
    fetch: async (_url, init) =>
      jsonResponse(200, init?.method === "POST" ? runningStatus() : failure),
  });
  const handle = client.statementHandle("stmt-1");
  assert.equal((await handle.cancel()).status, "running");
  await assert.rejects(handle.wait(), (error) => {
    assert.ok(error instanceof ScopeDBError);
    assert.equal(error.kind, "StatementFailed");
    assert.equal(error.code, "future_failure");
    assert.deepEqual(error.details, { phase: "commit" });
    return true;
  });
  const snapshot = handle.snapshot()!;
  if (snapshot.status === "failed") {
    (snapshot.error.details as { phase: string }).phase = "changed";
  }
  assert.deepEqual(handle.snapshot(), failure);
});

it("rejects mismatched statement identities without replacing a valid cache", async () => {
  let calls = 0;
  const client = new Client("https://example.com", {
    fetch: async () =>
      jsonResponse(
        200,
        runningStatus(calls++ ? "another-statement" : "stmt-1"),
      ),
  });
  const handle = client.statementHandle("stmt-1");
  await handle.refresh();
  await assert.rejects(handle.refresh(), /does not match/);
  assert.equal(handle.snapshot()?.statement_id, "stmt-1");
});

for (
  const scenario of [
    "lost acknowledgement",
    "unknown response",
    "unreadable success",
  ]
) {
  it(`does not replay ${scenario} by default`, async () => {
    let calls = 0;
    const client = new Client("https://example.com", {
      fetch: async () => {
        calls++;
        if (scenario === "lost acknowledgement") {
          throw new TypeError("connection closed after commit");
        }
        if (scenario === "unreadable success") {
          return new Response("interrupted");
        }
        return jsonResponse(503, {
          message: "unknown",
          code: "ingest_failure",
          details: { status: "unknown" },
        });
      },
    });
    const stream = client.table("events").appendStream().build();
    await stream.send({ id: 1 });
    await assert.rejects(
      stream.shutdown(),
      (error) =>
        error instanceof AppendRowsError && error.outcome === "unknown",
    );
    assert.equal(calls, 1);
    assert.equal(stream.stats().unknownRows, 1);
    assert.equal(stream.stats().retries, 0);
    assert.equal(stream.stats().pendingBytes, 0);
  });
}

it("honors server retry refusal even when unknown replay is enabled", async () => {
  let calls = 0;
  const client = new Client("https://example.com", {
    fetch: async () => {
      calls++;
      return jsonResponse(503, {
        error: {
          message: "do not replay",
          code: "ingest_failure",
          retryable: false,
          details: { status: "unknown", reason: "repair required" },
        },
      });
    },
  });
  const stream = client.table("events").appendStream().retryUnknown(true)
    .build();
  await stream.send({ id: 1 });
  await assert.rejects(stream.shutdown(), (error) => {
    assert.ok(error instanceof AppendRowsError);
    assert.equal(error.serverRetryable, false);
    assert.deepEqual(error.details, {
      status: "unknown",
      reason: "repair required",
    });
    assert.equal(stream.stats().lastFailure?.error, error);
    return true;
  });
  assert.equal(calls, 1);
});

it("round-trips nested catalog extensions with the original ETag", async () => {
  const definition = {
    name: "events",
    future_table_option: { mode: "new" },
    columns: [{ name: "id", data_type: "int", future_column_option: [1, 2] }],
  };
  const client = new Client("https://example.com", {
    fetch: async (_url, init) => {
      if (init?.method === "GET") {
        return jsonResponse(200, definition, { ETag: '"original"' });
      }
      assert.equal(new Headers(init?.headers).get("if-match"), '"original"');
      assert.deepEqual(JSON.parse(requestBodyText(init)), {
        ...definition,
        comment: "updated",
      });
      return jsonResponse(200, {
        before: definition,
        after: { ...definition, comment: "updated" },
        destructive: false,
      }, { ETag: '"next"' });
    },
  });
  const resource = client.table("events").catalog();
  const snapshot = await resource.get();
  snapshot.definition.comment = "updated";
  const applied = await resource.apply(snapshot.definition, {
    ifMatch: snapshot.etag,
  });
  assert.deepEqual(applied.after?.columns?.[0]?.["future_column_option"], [
    1,
    2,
  ]);
});

it("rejects malformed catalog reads and keeps malformed writes ambiguous", async () => {
  for (const method of ["get", "apply"] as const) {
    const invalid = { name: "events", columns: "invalid" };
    const client = new Client("https://example.com", {
      fetch: async () =>
        jsonResponse(
          200,
          method === "get"
            ? invalid
            : { before: null, after: invalid, destructive: false },
          { ETag: '"v1"' },
        ),
    });
    const resource = client.table("events").catalog();
    await assert.rejects(
      method === "get" ? resource.get() : resource.apply({ name: "events" }),
      (error) => {
        assert.ok(error instanceof ScopeDBError);
        assert.equal(
          error.code,
          method === "get" ? undefined : "catalog_commit_outcome_unknown",
        );
        assert.match(String(error.cause), /columns/);
        return true;
      },
    );
  }
});

it("does not infer a catalog rollback from an unstructured gateway failure", async () => {
  const client = new Client("https://example.com", {
    fetch: async () => jsonResponse(502, { message: "upstream unavailable" }),
  });
  await assert.rejects(
    client.table("events").catalog().delete({ allowDestructive: true }),
    (error) =>
      error instanceof ScopeDBError &&
      error.code === "catalog_commit_outcome_unknown" && !error.retryable,
  );
});

it("retains exact integer boundaries, special floats, and prototype-like column names", () => {
  const result = ResultSet.fromStatementResultSet({
    format: "json",
    metadata: {
      num_rows: 1,
      fields: [
        { name: "min", data_type: "int" },
        { name: "max", data_type: "uint" },
        { name: "nan", data_type: "float" },
        { name: "__proto__", data_type: "float" },
      ],
    },
    rows: [["-9223372036854775808", "18446744073709551615", "NaN", "-inf"]],
  });
  const row = result.first()!;
  assert.equal(row["min"], -(1n << 63n));
  assert.equal(row["max"], (1n << 64n) - 1n);
  assert.ok(Number.isNaN(row["nan"]));
  assert.equal(row["__proto__"], -Infinity);
  const invalid = ResultSet.fromStatementResultSet({
    format: "json",
    metadata: { num_rows: 1, fields: [{ name: "id", data_type: "uint" }] },
    rows: [["-1"]],
  });
  assert.throws(
    () => invalid.toValues(),
    (error) =>
      error instanceof ScopeDBError && error.context().get("column") === "id" &&
      /out of range/.test(String(error.cause)),
  );
});

it("classifies a row that can never fit the buffer separately from temporary backpressure", async () => {
  const stream = new Client("https://example.com").table("events")
    .appendStream().maxBufferedBytes(4).build();
  assert.equal(stream.trySend({ id: 1 }), false);
  assert.equal(stream.stats().droppedByReason.recordTooLarge, 1);
  assert.equal(stream.stats().droppedByReason.bufferFull, 0);
  await stream.shutdown();
});

it("rejects an oversized direct row count before making a request", async () => {
  let calls = 0;
  const client = new Client("https://example.com", {
    fetch: async () => {
      calls++;
      throw new Error("must not send");
    },
  });
  await assert.rejects(
    client.table("events").append("{}\n".repeat(200_001)),
    (error) => error instanceof AppendRowsError && error.outcome === "rejected",
  );
  assert.equal(calls, 0);
});

for (const [name, delayed, expected] of [
  [
    "keeps newer progress when an older response arrives late",
    runningWithProgress(0, 1), runningWithProgress(100, 2),
  ],
  [
    "uses request order to break equal progress timestamps",
    runningWithProgress(100, 1), runningWithProgress(100, 2),
  ],
  [
    "uses newer server progress even when its request was sent first",
    runningWithProgress(200, 3), runningWithProgress(200, 3),
  ],
  [
    "accepts a terminal result from an older request",
    finishedStatus(emptyResultSet()), finishedStatus(emptyResultSet()),
  ],
] as const) {
  it(name, async () => {
    const first = new Deferred<Response>();
    const started = new Deferred<void>();
    let requests = 0;
    const client = new Client("https://example.com", {
      fetch: async () => {
        if (++requests === 1) {
          started.resolve();
          return first.promise;
        }
        return jsonResponse(200, runningWithProgress(100, 2));
      },
    });
    const handle = client.statementHandle("stmt-1");
    const earlierRequest = handle.refresh();
    await started.promise;
    await handle.refresh();
    first.resolve(jsonResponse(200, delayed));

    assert.deepEqual(await earlierRequest, expected);
    assert.deepEqual(handle.snapshot(), expected);
    if (expected.status === "finished") {
      assert.equal((await handle.wait()).numRows(), 0);
    }
    assert.equal(requests, 2);
  });
}

function runningWithProgress(nanos: number, rows: number) {
  const snapshot = runningStatus();
  snapshot.progress.nanos_from_submitted = nanos;
  snapshot.progress.scanned_rows = rows;
  return snapshot;
}
