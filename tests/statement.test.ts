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

describe("StatementHandle.refresh", () => {
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
    const cancelled = cancelledStatus();
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
        assert.equal(err.statementId, handle.statementId);
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
        assert.equal(error.statementId, handle.statementId);
        return true;
      },
    );
    assert.equal(calls.length, 1);
  });

  it("throws ScopeDBError when statement is cancelled with in-band status", async () => {
    const { fn } = makeFetchStub([
      jsonResponse(200, cancelledStatus()),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1");

    await assert.rejects(
      () => handle.wait(noDelay),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        assert.equal(err.kind, "StatementCancelled");
        assert.equal(err.statementId, handle.statementId);
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
    const cancelResult = cancelledStatus();
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
    const cancelResult = cancelledStatus();
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
    const cancelled = cancelledStatus();
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
    const cancelResult = cancelledStatus();
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
      assert.equal(error.statementId, undefined);
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

for (const failure of ["HTTP 503", "transport"] as const) {
  it(`resumes a query after a polling ${failure} without submitting it again`, async () => {
    const methods: Array<string | undefined> = [];
    const cause = new TypeError("poll connection lost");
    const client = new Client("https://example.com", {
      fetch: async (url, init) => {
        methods.push(init?.method);
        if (methods.length === 1) return jsonResponse(200, runningStatus());
        assert.ok(String(url).endsWith("/statements/stmt-1?format=json"));
        if (methods.length === 2) {
          if (failure === "transport") throw cause;
          return jsonResponse(503, {
            code: "unavailable",
            message: "temporarily unavailable",
            retryable: true,
            details: { phase: "read" },
          }, { "X-Request-Id": "poll-request", "Retry-After": "2" });
        }
        return jsonResponse(200, finishedStatus(emptyResultSet()));
      },
    });
    let statementId: string | undefined;
    await assert.rejects(client.query("SELECT 1", noDelay), (error) => {
      assert.ok(error instanceof ScopeDBError);
      assert.equal(error.statementId, "stmt-1");
      assert.equal(error.retryable, false);
      assert.equal(error.isPersistent(), true);
      if (failure === "transport") {
        assert.equal(error.cause, cause);
      } else {
        assert.equal(error.code, "unavailable");
        assert.equal(error.message, "temporarily unavailable");
        assert.equal(error.httpStatus, 503);
        assert.equal(error.requestId, "poll-request");
        assert.equal(error.retryAfterMs, 2_000);
        assert.equal(error.serverRetryable, true);
        assert.deepEqual(error.details, { phase: "read" });
      }
      statementId = error.statementId;
      return true;
    });
    assert.deepEqual(methods, ["POST", "GET"]);
    assert.ok(statementId);
    const result = await client.statementHandle(statementId).wait(noDelay);
    assert.equal(result.numRows(), 0);
    assert.deepEqual(methods, ["POST", "GET", "GET"]);
  });
}

for (const method of ["refresh", "wait", "cancel"] as const) {
  it(`retains a handle's identity and temporary classification after ${method} fails`, async () => {
    const snapshot = runningStatus();
    const { fn, calls } = makeFetchStub([
      jsonResponse(503, { code: "unavailable", message: "try again" }),
      jsonResponse(200, finishedStatus(emptyResultSet())),
    ]);
    const client = new Client("https://example.com", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", snapshot);
    await assert.rejects(handle[method](), (error) => {
      assert.ok(error instanceof ScopeDBError);
      assert.equal(error.statementId, handle.statementId);
      assert.equal(error.retryable, true);
      return true;
    });
    assert.equal(calls.length, 1);
    assert.deepEqual(handle.snapshot(), snapshot);
    assert.equal((await handle.wait()).numRows(), 0);
    assert.deepEqual(calls.map(({ init }) => init?.method), [
      method === "cancel" ? "POST" : "GET",
      "GET",
    ]);
  });
}

it("retains the caller's statement ID after a lost submission acknowledgement", async () => {
  const id = "aabbccdd-1111-2222-3333-444455556666";
  const methods: Array<string | undefined> = [];
  const client = new Client("https://example.com", {
    fetch: async (url, init) => {
      methods.push(init?.method);
      if (init?.method === "POST") throw new TypeError("acknowledgement lost");
      assert.ok(String(url).endsWith(`/statements/${id}?format=json`));
      return jsonResponse(200, finishedStatus(emptyResultSet(), id));
    },
  });
  let statementId: string | undefined;
  await assert.rejects(
    client.statement("SELECT 1").withStatementId(`{${id.toUpperCase()}}`).submit(),
    (error) => {
      assert.ok(error instanceof ScopeDBError);
      assert.equal(error.code, "statement_submission_outcome_unknown");
      assert.equal(error.retryable, false);
      assert.equal(error.statementId, id);
      statementId = error.statementId;
      return true;
    },
  );
  assert.ok(statementId);
  assert.equal((await client.statementHandle(statementId).wait()).numRows(), 0);
  assert.deepEqual(methods, ["POST", "GET"]);
});

it("keeps a confirmed temporary submission rejection retryable through query", async () => {
  const { fn, calls } = makeFetchStub([
    jsonResponse(503, { code: "unavailable", message: "draining" }),
  ]);
  const client = new Client("https://example.com", { fetch: fn });
  await assert.rejects(client.query("SELECT 1"), (error) => {
    assert.ok(error instanceof ScopeDBError);
    assert.equal(error.retryable, true);
    assert.equal(error.statementId, undefined);
    return true;
  });
  assert.equal(calls.length, 1);
});

for (const operation of ["query", "submit", "cancel"] as const) {
  it(`preserves a caller-owned SDK error when ${operation} is aborted`, async () => {
    const controller = new AbortController();
    const started = new Deferred<void>();
    const reason = new ScopeDBError("Unexpected", "caller stopped")
      .setTemporary().withContext("statement_id", "earlier-statement");
    const client = new Client("https://example.com", {
      fetch: async (_url, init) => {
        if (operation === "query" && init?.method === "POST") {
          return jsonResponse(200, runningStatus());
        }
        started.resolve();
        return new Promise<Response>(() => {});
      },
    });
    const options = { signal: controller.signal };
    const pending = operation === "query"
      ? client.query("SELECT 1", options)
      : operation === "submit"
      ? client.statement("SELECT 1").withStatementId("stmt-1").submit(options)
      : client.statementHandle("stmt-1").cancel(options);
    await started.promise;
    controller.abort(reason);
    await assert.rejects(pending, (error: unknown) => error === reason);
    assert.equal(reason.retryable, true);
    assert.deepEqual([...reason.context()], [["statement_id", "earlier-statement"]]);
  });
}

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
