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
import { describe, it } from "node:test";
import { Client } from "../src/client.js";
import { ScopeDBError } from "../src/errors.js";
import { StatementHandle } from "../src/statement.js";
import { Deferred } from "../src/stream-internals.js";
import {
  cancelledStatus,
  emptyResultSet,
  failedStatus,
  finishedStatus,
  jsonResponse,
  makeFetchStub,
  parseJsonRequestBody,
  pendingStatus,
  runningStatus,
} from "./helpers.js";

// Use zero delays in tests to avoid slow polling
const noDelay = { initialDelayMs: 0, maxDelayMs: 0 };

describe("Statement.submit", () => {
  it("submits statement and returns a handle with correct statementId", async () => {
    const status = pendingStatus("stmt-abc");
    const { fn } = makeFetchStub([jsonResponse(200, status)]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    const handle = await client.statement("SELECT 1").submit();

    assert.equal(handle.statementId, "stmt-abc");
    assert.equal(handle.snapshot()?.status, "pending");
  });

  it("forwards optional fields from Statement builder", async () => {
    const status = pendingStatus();
    const { fn, calls } = makeFetchStub([
      jsonResponse(200, { ...status, statement_id: "custom-id" }),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await client
      .statement("SELECT 1")
      .withStatementId("custom-id")
      .withMaxElapsedSeconds(60).withMaxEstimatedSeconds(3)
      .submit();

    const body = parseJsonRequestBody(calls[0]!.init) as Record<
      string,
      unknown
    >;
    assert.equal(body["statement_id"], "custom-id");
    assert.equal(body["max_elapsed_seconds"], 60);
    assert.equal(body["max_estimated_seconds"], 3);
    assert.equal(body["format"], "json");
  });
});

describe("StatementHandle.status", () => {
  it("fetches status from server and updates the local snapshot", async () => {
    const status = runningStatus("stmt-1");
    const { fn, calls } = makeFetchStub([jsonResponse(200, status)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1");

    const latest = await handle.refresh();

    assert.equal(calls.length, 1);
    assert.equal(latest.status, "running");
    assert.deepEqual(handle.snapshot(), latest);
  });

  it("snapshot returns the cached snapshot without making a request", async () => {
    const status = runningStatus("stmt-1");
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", status);

    const snapshot = handle.snapshot();

    assert.deepEqual(snapshot, status);
    assert.equal(calls.length, 0);
  });

  it("skips the request when already finished", async () => {
    const finished = finishedStatus(emptyResultSet());
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", finished);

    const status = await handle.refresh();

    assert.equal(calls.length, 0);
    assert.deepEqual(status, finished);
  });

  it("skips the request when already failed", async () => {
    const failed = failedStatus("something went wrong");
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", failed);

    const status = await handle.refresh();

    assert.equal(calls.length, 0);
    assert.deepEqual(status, failed);
  });

  it("skips the request when already cancelled", async () => {
    const cancelled = cancelledStatus("user cancelled");
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", cancelled);

    const status = await handle.refresh();

    assert.equal(calls.length, 0);
    assert.deepEqual(status, cancelled);
  });
});

describe("StatementHandle.wait", () => {
  it("returns ResultSet immediately when first poll returns finished", async () => {
    const resultSet = {
      metadata: {
        fields: [{ name: "x", data_type: "int" as const }],
        num_rows: 1,
      },
      format: "json" as const,
      rows: [["42"]],
    };
    const finished = finishedStatus(resultSet);
    const { fn, calls } = makeFetchStub([jsonResponse(200, finished)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1");

    const rs = await handle.wait(noDelay);

    assert.equal(calls.length, 1);
    assert.equal(rs.numRows(), 1);
  });

  it("polls through pending→running→finished and returns ResultSet", async () => {
    const finished = finishedStatus(emptyResultSet());
    const { fn, calls } = makeFetchStub([
      jsonResponse(200, pendingStatus()),
      jsonResponse(200, runningStatus()),
      jsonResponse(200, finished),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1");

    const rs = await handle.wait(noDelay);

    assert.equal(calls.length, 3);
    assert.equal(rs.numRows(), 0);
  });

  it("waits until the statement finishes", async () => {
    const finished = finishedStatus(emptyResultSet());
    const { fn } = makeFetchStub([jsonResponse(200, finished)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1");

    const rs = await handle.wait(noDelay);

    assert.equal(rs.numRows(), 0);
  });

  it("throws ScopeDBError when statement fails with in-band status", async () => {
    const details = {
      code: "row_limit_exceeded",
      message: "query returned too many rows",
      details: { total_rows: 101, max_total_rows: 100 },
    };
    const { fn } = makeFetchStub([
      jsonResponse(200, failedStatus("query error", "stmt-1", details)),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1");

    await assert.rejects(
      () => handle.wait(noDelay),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        assert.equal(err.kind, "StatementFailed");
        assert.ok(
          err.message.includes("query returned too many rows"),
          `message was: ${err.message}`,
        );
        assert.deepEqual(err.statementDetails, details);
        return true;
      },
    );
  });

  it("rejects an unknown statement status instead of polling again", async () => {
    const malformed = { ...pendingStatus(), status: "paused" };
    const { fn, calls } = makeFetchStub([jsonResponse(200, malformed)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1");

    await assert.rejects(
      () => handle.wait(noDelay),
      (error: unknown) => {
        assert.ok(error instanceof ScopeDBError);
        assert.match(error.message, /statement response has an invalid body/);
        return true;
      },
    );
    assert.equal(calls.length, 1);
  });

  it("throws ScopeDBError when statement is cancelled with in-band status", async () => {
    const { fn } = makeFetchStub([
      jsonResponse(200, cancelledStatus("cancelled by user")),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1");

    await assert.rejects(
      () => handle.wait(noDelay),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        assert.equal(err.kind, "StatementCancelled");
        assert.ok(
          err.message.includes("is cancelled"),
          `message was: ${err.message}`,
        );
        return true;
      },
    );
  });

  it("respects AbortSignal and throws on abort", async () => {
    // pending forever — the signal will abort it
    const { fn } = makeFetchStub([
      jsonResponse(200, pendingStatus()),
      jsonResponse(200, pendingStatus()),
      jsonResponse(200, pendingStatus()),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1");
    const controller = new AbortController();

    const promise = handle.wait({
      initialDelayMs: 5,
      maxDelayMs: 10,
      signal: controller.signal,
    });

    // Abort after first fetch completes
    setTimeout(() => controller.abort(), 1);

    await assert.rejects(promise);
  });

  it("keeps fetch as a deprecated forwarding alias", async () => {
    const finished = finishedStatus(emptyResultSet());
    const { fn } = makeFetchStub([jsonResponse(200, finished)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1");

    const result = await handle.wait(noDelay);

    assert.equal(result.numRows(), 0);
  });
});

describe("StatementHandle.cancel", () => {
  it("sends POST cancel when statement is still pending", async () => {
    const cancelResult = cancelledStatus("cancelled");
    const pending = pendingStatus();
    const { fn, calls } = makeFetchStub([jsonResponse(200, cancelResult)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", pending);

    const result = await handle.cancel();

    assert.equal(calls.length, 1);
    assert.ok(
      calls[0]!.url.endsWith("/cancel?format=json"),
      `unexpected URL: ${calls[0]!.url}`,
    );
    assert.equal(result.status, "cancelled");
  });

  it("sends POST cancel when statement is running", async () => {
    const cancelResult = cancelledStatus("cancelled");
    const { fn, calls } = makeFetchStub([jsonResponse(200, cancelResult)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", runningStatus());

    await handle.cancel();

    assert.equal(calls.length, 1);
  });

  it("returns cached status without HTTP call when already finished", async () => {
    const finished = finishedStatus(emptyResultSet());
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", finished);

    const result = await handle.cancel();

    assert.equal(calls.length, 0);
    assert.equal(result.status, "finished");
  });

  it("returns cached status without HTTP call when already failed", async () => {
    const failed = failedStatus("something went wrong");
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", failed);

    const result = await handle.cancel();

    assert.equal(calls.length, 0);
    assert.equal(result.status, "failed");
  });

  it("returns cached status without HTTP call when already cancelled", async () => {
    const cancelled = cancelledStatus("already cancelled");
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", cancelled);

    const result = await handle.cancel();

    assert.equal(calls.length, 0);
    assert.equal(result.status, "cancelled");
  });

  it("updates internal status after cancel returns failed", async () => {
    const cancelResult = failedStatus("execution failed");
    const { fn } = makeFetchStub([jsonResponse(200, cancelResult)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", pendingStatus());

    await handle.cancel();

    assert.equal(handle.snapshot()?.status, "failed");
  });

  it("updates internal status after cancel returns cancelled", async () => {
    const cancelResult = cancelledStatus("cancelled");
    const { fn } = makeFetchStub([jsonResponse(200, cancelResult)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", runningStatus());

    await handle.cancel();

    assert.equal(handle.snapshot()?.status, "cancelled");
  });
});

it("retains the full result when completion races with cancellation", async () => {
  const response = finishedStatus({
    metadata: { fields: [{ name: "id", data_type: "int" }], num_rows: 1 },
    format: "json",
    rows: [["7"]],
  });
  response.progress.compute_seconds = 0.125;
  const { fn, calls } = makeFetchStub([jsonResponse(200, response)]);
  const handle = new Client("https://example.com", { fetch: fn })
    .statementHandle("stmt-1");
  assert.deepEqual(await handle.cancel(), response);
  assert.equal(handle.snapshot()?.progress?.compute_seconds, 0.125);
  assert.equal((await handle.wait()).toObjects()[0]?.["id"], 7n);
  assert.equal(calls.length, 1);
});

it("keeps the statement ID captured for each submission", async () => {
  const started = new Deferred<void>();
  const response = new Deferred<Response>();
  const client = new Client("https://example.com", {
    fetch: async () => {
      started.resolve();
      return response.promise;
    },
  });
  const statement = client.statement("SELECT 1").withStatementId("stmt-a");
  const submitting = statement.submit();
  await started.promise;
  statement.withStatementId("stmt-b");
  response.resolve(jsonResponse(200, pendingStatus("stmt-a")));

  const handle = await submitting;
  assert.equal(handle.statementId, "stmt-a");
  assert.equal(handle.snapshot()?.statement_id, "stmt-a");
});

it("accepts equivalent UUID spellings for statement handles", async () => {
  const canonical = "01890a5d-4ac8-7c3f-b8a2-2f50f2dca7c4";
  for (const supplied of [canonical.toUpperCase(), canonical.replaceAll("-", "")]) {
    const { fn } = makeFetchStub([
      jsonResponse(200, pendingStatus(canonical)),
      jsonResponse(200, runningStatus(canonical)),
    ]);
    const client = new Client("https://example.com", { fetch: fn });
    const handle = await client.statement("SELECT 1")
      .withStatementId(supplied)
      .submit();

    assert.equal(handle.statementId, canonical);
    assert.equal((await handle.refresh()).statement_id, canonical);
  }
});

it("settles statement waits with the caller reason when fetch ignores abort", { timeout: 1_000 }, async () => {
  const started = new Deferred<void>();
  const client = new Client("https://example.com", {
    fetch: async () => {
      started.resolve();
      return new Promise<Response>(() => {});
    },
  });
  const controller = new AbortController();
  const reason = new Error("caller stopped waiting");
  const waiting = client.statementHandle("stmt-1").refresh({
    signal: controller.signal,
  });
  await started.promise;
  controller.abort(reason);

  await assert.rejects(waiting, (error: unknown) => error === reason);
});

for (const status of [undefined, 408, 429, 502]) {
  it(`does not mark an unacknowledged submission as retryable (${status ?? "transport"})`, async () => {
    let calls = 0;
    const client = new Client("https://example.com", {
      fetch: async () => {
        calls++;
        if (status === undefined) throw new TypeError("connection lost after acceptance");
        return new Response("gateway failure", { status });
      },
    });
    await assert.rejects(client.statement("FROM incoming_events SELECT id INSERT INTO events").submit(), (error) => {
      assert.ok(error instanceof ScopeDBError);
      assert.equal(error.retryable, false);
      assert.equal(error.code, "statement_submission_outcome_unknown");
      assert.equal(error.httpStatus, status);
      assert.ok(error.cause instanceof ScopeDBError);
      return true;
    });
    assert.equal(calls, 1);
  });
}

it("preserves a confirmed temporary statement admission rejection", async () => {
  const client = new Client("https://example.com", {
    fetch: async () => jsonResponse(503, { code: "unavailable", message: "draining" }),
  });
  await assert.rejects(client.statement("SELECT 1").submit(), (error) => {
    assert.ok(error instanceof ScopeDBError);
    assert.equal(error.code, "unavailable");
    assert.equal(error.retryable, true);
    return true;
  });
});

for (const status of [200, 503]) {
  it(`preserves the caller abort reason while reading a ${status} statement response`, async () => {
    const controller = new AbortController();
    const reason = new Error("caller stopped reading");
    const started = new Deferred<void>();
    const client = new Client("https://example.com", {
      fetch: async () => {
        const body = new ReadableStream<Uint8Array>({
          start(stream) {
            controller.signal.addEventListener("abort", () => {
              stream.error(reason);
            }, { once: true });
          },
        });
        started.resolve();
        return new Response(body, { status });
      },
    });
    const waiting = client.statementHandle("stmt-1").refresh({
      signal: controller.signal,
    });
    await started.promise;
    controller.abort(reason);

    await assert.rejects(waiting, (error: unknown) => error === reason);
  });
}
