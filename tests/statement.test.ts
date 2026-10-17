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
import {
  cancelledStatus,
  emptyProgress,
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
    assert.equal(handle.lastStatus()?.status, "pending");
  });

  it("forwards optional fields from Statement builder", async () => {
    const status = pendingStatus();
    const { fn, calls } = makeFetchStub([jsonResponse(200, status)]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await client
      .statement("SELECT 1")
      .withStatementId("custom-id")
      .withExecTimeout("60s")
      .submit();

    const body = parseJsonRequestBody(calls[0]!.init) as Record<string, unknown>;
    assert.equal(body["statement_id"], "custom-id");
    assert.equal(body["exec_timeout"], "60s");
    assert.equal(body["max_parallelism"], undefined);
    assert.equal(body["format"], "json");
  });
});

describe("StatementHandle.status", () => {
  it("fetches status from server and updates the local snapshot", async () => {
    const status = runningStatus("stmt-1");
    const { fn, calls } = makeFetchStub([jsonResponse(200, status)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1");

    const latest = await handle.status();

    assert.equal(calls.length, 1);
    assert.equal(latest.status, "running");
    assert.equal(handle.lastStatus(), latest);
  });

  it("lastStatus returns the cached snapshot without making a request", async () => {
    const status = runningStatus("stmt-1");
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", "json", status);

    const snapshot = handle.lastStatus();

    assert.equal(snapshot, status);
    assert.equal(calls.length, 0);
  });

  it("skips the request when already finished", async () => {
    const finished = finishedStatus(emptyResultSet());
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", "json", finished);

    const status = await handle.status();

    assert.equal(calls.length, 0);
    assert.equal(status, finished);
  });

  it("skips the request when already failed", async () => {
    const failed = failedStatus("something went wrong");
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", "json", failed);

    const status = await handle.status();

    assert.equal(calls.length, 0);
    assert.equal(status, failed);
  });

  it("skips the request when already cancelled", async () => {
    const cancelled = cancelledStatus("user cancelled");
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", "json", cancelled);

    const status = await handle.status();

    assert.equal(calls.length, 0);
    assert.equal(status, cancelled);
  });

  it("keeps fetchOnce as a deprecated forwarding alias", async () => {
    const status = runningStatus("stmt-1");
    const { fn, calls } = makeFetchStub([jsonResponse(200, status)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1");

    await handle.fetchOnce();

    assert.equal(calls.length, 1);
    assert.deepEqual(handle.lastStatus(), status);
  });
});

describe("StatementHandle.wait", () => {
  it("returns ResultSet immediately when first poll returns finished", async () => {
    const resultSet = {
      metadata: { fields: [{ name: "x", data_type: "int" as const }], num_rows: 1 },
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
        assert.ok(err.message.includes("query error"), `message was: ${err.message}`);
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
    const { fn } = makeFetchStub([jsonResponse(200, cancelledStatus("cancelled by user"))]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1");

    await assert.rejects(
      () => handle.wait(noDelay),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        assert.equal(err.kind, "StatementFailed");
        assert.ok(err.message.includes("cancelled by user"), `message was: ${err.message}`);
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

    const promise = handle.wait({ initialDelayMs: 5, maxDelayMs: 10, signal: controller.signal });

    // Abort after first fetch completes
    setTimeout(() => controller.abort(), 1);

    await assert.rejects(promise);
  });

  it("keeps fetch as a deprecated forwarding alias", async () => {
    const finished = finishedStatus(emptyResultSet());
    const { fn } = makeFetchStub([jsonResponse(200, finished)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1");

    const result = await handle.fetch(noDelay);

    assert.equal(result.numRows(), 0);
  });
});

describe("StatementHandle.cancel", () => {
  it("sends POST cancel when statement is still pending", async () => {
    const cancelResult = {
      statement_id: "stmt-1",
      created_at: "2024-01-01T00:00:00Z",
      status: "cancelled" as const,
      message: "cancelled",
    };
    const pending = pendingStatus();
    const { fn, calls } = makeFetchStub([jsonResponse(200, cancelResult)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", "json", pending);

    const result = await handle.cancel();

    assert.equal(calls.length, 1);
    assert.ok(calls[0]!.url.endsWith("/cancel?format=json"), `unexpected URL: ${calls[0]!.url}`);
    assert.equal(result.status, "cancelled");
  });

  it("sends POST cancel when statement is running", async () => {
    const cancelResult = {
      statement_id: "stmt-1",
      created_at: "2024-01-01T00:00:00Z",
      status: "cancelled" as const,
      message: "cancelled",
    };
    const { fn, calls } = makeFetchStub([jsonResponse(200, cancelResult)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", "json", runningStatus());

    await handle.cancel();

    assert.equal(calls.length, 1);
  });

  it("returns synthetic result without HTTP call when already finished", async () => {
    const finished = finishedStatus(emptyResultSet());
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", "json", finished);

    const result = await handle.cancel();

    assert.equal(calls.length, 0);
    assert.equal(result.status, "finished");
  });

  it("returns synthetic result without HTTP call when already failed", async () => {
    const failed = failedStatus("something went wrong");
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", "json", failed);

    const result = await handle.cancel();

    assert.equal(calls.length, 0);
    assert.equal(result.status, "failed");
  });

  it("returns synthetic result without HTTP call when already cancelled", async () => {
    const cancelled = cancelledStatus("already cancelled");
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", "json", cancelled);

    const result = await handle.cancel();

    assert.equal(calls.length, 0);
    assert.equal(result.status, "cancelled");
  });

  for (const status of ["finished", "failed"] as const) {
    it(`fetches the complete ${status} snapshot after a short 0.5 cancel response`, async () => {
      const cancelResult = {
        statement_id: "stmt-1", created_at: "2024-01-01T00:00:00Z",
        status, message: `statement is ${status}`,
      };
      const details = { code: "future_failure", message: "actual failure", details: { limit: 2 } };
      const snapshot = status === "finished"
        ? finishedStatus(emptyResultSet()) : failedStatus("actual failure", "stmt-1", details);
      const { fn, calls } = makeFetchStub([
        jsonResponse(200, cancelResult), jsonResponse(200, snapshot),
      ]);
      const handle = new Client("http://localhost:8080", { fetch: fn }).statementHandle("stmt-1");
      const cancelled = await handle.cancel();
      assert.equal(handle.lastStatus(), undefined);
      assert.deepEqual(await handle.cancel(), cancelled);
      if (status === "finished") {
        assert.equal((await handle.wait(noDelay)).numRows(), 0);
      } else {
        await assert.rejects(handle.wait(noDelay), (error: unknown) => {
          assert.ok(error instanceof ScopeDBError);
          assert.equal(error.message, "actual failure");
          assert.deepEqual(error.statementDetails, details);
          return true;
        });
      }
      assert.deepEqual(await handle.cancel(), cancelled);
      assert.equal(calls.length, 2);
    });
  }

  for (const status of ["finished", "failed", "cancelled"] as const) {
    it(`caches the complete 0.6 ${status} cancellation without fetching again`, async () => {
      const progress = { ...emptyProgress(), scanned_rows: 17 };
      const details = { code: "future_failure", message: "actual failure", details: { limit: 2 } };
      const snapshot = {
        statement_id: "stmt-1", created_at: "2024-01-01T00:00:00Z", progress, status,
        ...(status === "finished" ? { result_set: emptyResultSet() } : {}),
        ...(status === "failed" ? { error: details } : {}),
      };
      const { fn, calls } = makeFetchStub([jsonResponse(200, snapshot)]);
      const handle = new Client("http://localhost:8080", { fetch: fn }).statementHandle("stmt-1");
      const cancelled = await handle.cancel();
      assert.equal(cancelled.status, status);
      assert.equal((await handle.status()).status, status);
      assert.deepEqual(handle.progress(), progress);
      if (status === "finished") {
        assert.equal((await handle.wait(noDelay)).numRows(), 0);
      } else {
        await assert.rejects(handle.wait(noDelay), (error: unknown) => {
          assert.ok(error instanceof ScopeDBError);
          assert.equal(error.kind, "StatementFailed");
          assert.equal(error.message, status === "failed" ? "actual failure" : "statement is cancelled");
          assert.deepEqual(error.statementDetails, status === "failed" ? details : undefined);
          return true;
        });
      }
      assert.equal(await handle.cancel(), cancelled);
      assert.equal(calls.length, 1);
    });
  }

  it("updates internal status after cancel returns cancelled", async () => {
    const cancelResult = {
      statement_id: "stmt-1",
      created_at: "2024-01-01T00:00:00Z",
      status: "cancelled" as const,
      message: "done",
    };
    const { fn } = makeFetchStub([jsonResponse(200, cancelResult)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", "json", runningStatus());

    await handle.cancel();

    assert.equal(handle.lastStatus()?.status, "cancelled");
  });
});

describe("0.6 statement responses", () => {
  for (const operation of ["submit", "fetch"] as const) {
    it(`preserves failures without a top-level message from ${operation}`, async () => {
      const error = { code: "future_failure", message: "specific failure", details: { a: 1 } };
      const { fn } = makeFetchStub([jsonResponse(200, {
        ...pendingStatus(), status: "failed", error,
      })]);
      const client = new Client("http://localhost:8080", { fetch: fn });
      const handle = operation === "submit"
        ? await client.statement("invalid").submit() : client.statementHandle("stmt-1");
      await assert.rejects(handle.wait(noDelay), (cause: unknown) => {
        assert.ok(cause instanceof ScopeDBError);
        assert.equal(cause.message, error.message);
        assert.equal(cause.code, error.code);
        assert.deepEqual(cause.statementDetails, error);
        return true;
      });
    });
  }

  it("rejects malformed cancellation snapshots instead of caching success", async () => {
    for (const body of [
      { ...pendingStatus(), status: "finished", message: "finished" },
      { ...pendingStatus(), status: "failed", error: { message: "missing code" } },
      { ...pendingStatus(), status: "cancelled", progress: null },
    ]) {
      const { fn } = makeFetchStub([jsonResponse(200, body)]);
      const handle = new Client("http://localhost:8080", { fetch: fn }).statementHandle("stmt-1");
      await assert.rejects(handle.cancel(), /invalid body/);
      assert.equal(handle.lastStatus(), undefined);
    }
  });
});

describe("Statement.executeOne", () => {
  it("returns first row as object when result set is non-empty", async () => {
    const rs = {
      metadata: {
        fields: [{ name: "count", data_type: "int" as const }],
        num_rows: 1,
      },
      format: "json" as const,
      rows: [["42"]],
    };
    const { fn } = makeFetchStub([
      jsonResponse(200, pendingStatus()),
      jsonResponse(200, finishedStatus(rs)),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    const row = await client.statement("FROM t AGGREGATE count() AS count").executeOne(noDelay);

    assert.ok(row !== null);
    assert.equal(row["count"], 42n);
  });

  it("returns null when result set is empty", async () => {
    const rs = {
      metadata: { fields: [{ name: "x", data_type: "int" as const }], num_rows: 0 },
      format: "json" as const,
      rows: [],
    };
    const { fn } = makeFetchStub([jsonResponse(200, finishedStatus(rs))]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    const row = await client.statement("SELECT 1 WHERE false").executeOne(noDelay);
    assert.equal(row, null);
  });
});
