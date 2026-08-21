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
import type {
  AppendStream,
  AppendStreamOptions,
} from "../src/append-stream.js";
import { Client } from "../src/client.js";
import { AppendRowsError, ScopeDBError } from "../src/errors.js";
import type { FetchCall } from "./helpers.js";
import { jsonResponse, makeFetchStub, requestBodyText } from "./helpers.js";

function appendOk(numRows = 1): Response {
  return jsonResponse(200, {
    append_state: "committed",
    num_rows_inserted: numRows,
  });
}

function appendRejected(status = 503): Response {
  return jsonResponse(status, {
    message: "append is temporarily unavailable",
    append_state: "rejected",
    row_errors: [],
    row_errors_truncated: false,
  });
}

function appendUnknown(): Response {
  return jsonResponse(503, {
    message: "append commit outcome is unknown",
    append_state: "unknown",
    row_errors: [],
    row_errors_truncated: false,
  });
}

function makeTable(responses: Response[]) {
  const { fn, calls } = makeFetchStub(responses);
  const client = new Client("http://localhost:8080", { fetch: fn });
  const table = client
    .table("events")
    .withDatabase("analytics")
    .withSchema("public");
  return { table, calls };
}

function parseAppendRows(call: FetchCall): unknown[] {
  return requestBodyText(call.init)
    .split("\n")
    .map((line) => JSON.parse(line) as unknown);
}

describe("AppendStream batching and barriers", () => {
  it("batches records and aggregates results between flush barriers", async () => {
    const { table, calls } = makeTable([appendOk(3)]);
    const stream = table.appendStream().batchBytes(1024).build();

    await stream.send({ id: 1 });
    await stream.send({ id: 2 });
    await stream.send({ id: 3 });

    assert.deepEqual(await stream.flush(), {
      append_state: "committed",
      num_rows_inserted: 3,
    });
    assert.equal(await stream.flush(), null);
    assert.equal(await stream.shutdown(), null);

    assert.equal(calls.length, 1);
    assert.equal(
      new Headers(calls[0]!.init?.headers).get("Content-Encoding"),
      "gzip",
    );
    assert.deepEqual(parseAppendRows(calls[0]!), [
      { id: 1 },
      { id: 2 },
      { id: 3 },
    ]);
  });

  it("seals a batch before the next record would exceed the target size", async () => {
    const first = { id: 1 };
    const second = { id: 2 };
    const third = { id: 3 };
    const batchBytes = Buffer.byteLength(JSON.stringify(first), "utf8") +
      1 + Buffer.byteLength(JSON.stringify(second), "utf8");
    const { table, calls } = makeTable([appendOk(2), appendOk(1)]);
    const stream = table.appendStream()
      .batchBytes(batchBytes)
      .maxInFlightRequests(1)
      .build();

    await stream.send(first);
    await stream.send(second);
    await stream.send(third);
    const result = await stream.flush();
    await stream.shutdown();

    assert.equal(result?.num_rows_inserted, 3);
    assert.equal(calls.length, 2);
    assert.deepEqual(parseAppendRows(calls[0]!), [first, second]);
    assert.deepEqual(parseAppendRows(calls[1]!), [third]);
    assert.ok(
      Buffer.byteLength(requestBodyText(calls[0]!.init), "utf8") <= batchBytes,
    );
  });

  it("sends an oversized-for-target row alone when it is within the protocol limit", async () => {
    const row = { payload: "larger than one byte" };
    const { table, calls } = makeTable([appendOk(1)]);
    const stream = table.appendStream().batchBytes(1).build();

    await stream.send(row);
    await stream.shutdown();

    assert.deepEqual(parseAppendRows(calls[0]!), [row]);
  });

  it("never exceeds the append protocol's 200,000-row request limit", async () => {
    const { table, calls } = makeTable([appendOk(200_000), appendOk(1)]);
    const stream = table.appendStream()
      .flushInterval(60_000)
      .maxConcurrentBatches(1)
      .build();

    for (let index = 0; index < 200_001; index += 1) {
      await stream.send({});
    }
    await stream.shutdown();

    assert.equal(calls.length, 2);
    assert.deepEqual(
      calls.map((call) => requestBodyText(call.init).split("\n").length),
      [200_000, 1],
    );
  });

  it("supports ergonomic batching and concurrency configuration names", async () => {
    const { table, calls } = makeTable([appendOk(2), appendOk(2), appendOk(1)]);
    const stream = table.appendStream()
      .targetBatchBytes(1024)
      .maxBatchRows(2)
      .flushIntervalMs(60_000)
      .maxConcurrentBatches(1)
      .maxBufferedBytes(1024 * 1024)
      .attemptTimeoutMs(10_000)
      .build();

    await stream.sendAll([
      { id: 1 },
      { id: 2 },
      { id: 3 },
      { id: 4 },
      { id: 5 },
    ]);

    assert.equal((await stream.shutdown())?.num_rows_inserted, 5);
    assert.deepEqual(calls.map(parseAppendRows), [
      [{ id: 1 }, { id: 2 }],
      [{ id: 3 }, { id: 4 }],
      [{ id: 5 }],
    ]);
  });

  it("flushes by the first-row deadline even while records keep arriving", async () => {
    const { table, calls } = makeTable([appendOk(2)]);
    const stream = table.appendStream()
      .batchBytes(1024)
      .flushInterval(30)
      .build();

    await stream.send({ id: 1 });
    await delay(15);
    await stream.send({ id: 2 });
    await waitForCallCount(calls, 1);

    assert.deepEqual(parseAppendRows(calls[0]!), [{ id: 1 }, { id: 2 }]);
    assert.equal((await stream.shutdown())?.num_rows_inserted, 2);
  });

  it("shutdown is idempotent, flushes remaining rows, and rejects later sends", async () => {
    const { table, calls } = makeTable([appendOk(1)]);
    const stream = table.appendStream().build();
    await stream.send({ id: 1 });

    const first = stream.shutdown();
    const second = stream.shutdown();

    assert.equal(first, second);
    assert.equal((await first)?.num_rows_inserted, 1);
    assert.equal(calls.length, 1);
    await assert.rejects(() => stream.send({ id: 2 }), ScopeDBError);
  });

  it("snapshots the target table when the builder is created", async () => {
    const { fn, calls } = makeFetchStub([appendOk(1)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const table = client.table("events").withSchema("before");
    const builder = table.appendStream();
    table.withSchema("after");
    const stream = builder.build();

    await stream.send({ id: 1 });
    await stream.shutdown();

    assert.ok(calls[0]!.url.includes("/schemas/before/"), calls[0]!.url);
  });
});

describe("AppendStream concurrency", () => {
  it("runs up to the configured number of append requests concurrently", async () => {
    const calls: FetchCall[] = [];
    const gates: Array<TestDeferred<void>> = [];
    let active = 0;
    let maxActive = 0;

    const fetch: typeof globalThis.fetch = async (input, init) => {
      calls.push({ url: input.toString(), init });
      const gate = new TestDeferred<void>();
      gates.push(gate);
      active += 1;
      maxActive = Math.max(maxActive, active);
      await gate.promise;
      active -= 1;
      return appendOk(1);
    };
    const client = new Client("http://localhost:8080", { fetch });
    const stream = client.table("events")
      .appendStream()
      .batchBytes(1)
      .maxInFlightRequests(2)
      .build();

    await stream.send({ id: 1 });
    await stream.send({ id: 2 });
    await stream.send({ id: 3 });
    const flushing = stream.flush();

    await waitForCallCount(calls, 2);
    assert.equal(maxActive, 2);
    assert.equal(calls.length, 2);

    gates[0]!.resolve();
    await waitForCallCount(calls, 3);
    assert.equal(maxActive, 2);
    gates[1]!.resolve();
    gates[2]!.resolve();

    assert.equal((await flushing)?.num_rows_inserted, 3);
    await stream.shutdown();
  });

  it("releases blocked producers on fatal but keeps flush pending until all in-flight appends settle", async () => {
    const calls: FetchCall[] = [];
    const fatalGate = new TestDeferred<void>();
    const successfulGate = new TestDeferred<void>();
    let callIndex = 0;

    const fetch: typeof globalThis.fetch = async (input, init) => {
      const index = callIndex++;
      calls.push({ url: input.toString(), init });
      if (index === 0) {
        await fatalGate.promise;
        return appendUnknown();
      }
      await successfulGate.promise;
      return appendOk(1);
    };
    const client = new Client("http://localhost:8080", { fetch });
    const rowBytes = Buffer.byteLength(JSON.stringify({ id: 1 }), "utf8") + 1;
    const stream = client.table("events")
      .appendStream()
      .batchBytes(1)
      .maxInFlightRequests(2)
      .maxPendingBytes(rowBytes * 2)
      .maxRetries(0)
      .build();

    await stream.send({ id: 1 });
    await stream.send({ id: 2 });
    await waitForCallCount(calls, 2);

    const blockedSend = stream.send({ id: 3 });
    let sendSettled = false;
    void blockedSend.then(
      () => { sendSettled = true; },
      () => { sendSettled = true; },
    );
    await delay(0);
    assert.equal(sendSettled, false);

    const flushing = stream.flush();
    let flushSettled = false;
    void flushing.then(
      () => { flushSettled = true; },
      () => { flushSettled = true; },
    );

    fatalGate.resolve();
    await assert.rejects(blockedSend, AppendRowsError);
    assert.equal(flushSettled, false);

    const lateFlush = stream.flush();
    let lateFlushSettled = false;
    void lateFlush.then(
      () => { lateFlushSettled = true; },
      () => { lateFlushSettled = true; },
    );
    await delay(0);
    assert.equal(lateFlushSettled, false);

    successfulGate.resolve();
    await assert.rejects(
      flushing,
      (error: unknown) => {
        assert.ok(error instanceof AppendRowsError);
        assert.equal(error.context().get("committed_batches"), "1");
        assert.equal(error.context().get("num_rows_inserted"), "1");
        return true;
      },
    );
    await assert.rejects(lateFlush, AppendRowsError);
    await assert.rejects(() => stream.shutdown(), AppendRowsError);
  });

  it("promotes a strict barrier error to unknown when a later concurrent batch is ambiguous", async () => {
    const calls: FetchCall[] = [];
    const rejectedGate = new TestDeferred<void>();
    const unknownGate = new TestDeferred<void>();
    let callIndex = 0;
    const fetch: typeof globalThis.fetch = async (input, init) => {
      const index = callIndex++;
      calls.push({ url: input.toString(), init });
      if (index === 0) {
        await rejectedGate.promise;
        return appendRejected(400);
      }
      await unknownGate.promise;
      return appendUnknown();
    };
    const client = new Client("http://localhost:8080", { fetch });
    const stream = client.table("events")
      .appendStream()
      .batchBytes(1)
      .maxInFlightRequests(2)
      .maxRetries(0)
      .build();

    await stream.send({ id: 1 });
    await stream.send({ id: 2 });
    await waitForCallCount(calls, 2);
    const flushing = stream.flush();

    rejectedGate.resolve();
    await delay(0);
    unknownGate.resolve();

    await assert.rejects(
      flushing,
      (error: unknown) => {
        assert.ok(error instanceof AppendRowsError);
        assert.equal(error.appendState, "unknown");
        assert.equal(error.context().get("failed_rows"), "1");
        assert.equal(error.context().get("unknown_rows"), "1");
        return true;
      },
    );
    assert.equal(stream.stats().failedRows, 1);
    assert.equal(stream.stats().unknownRows, 1);
    await assert.rejects(
      () => stream.shutdown(),
      (error: unknown) =>
        error instanceof AppendRowsError && error.appendState === "unknown",
    );
  });
});

describe("AppendStream retry safety", () => {
  it("retries only an explicitly rejected temporary append with the same payload", async () => {
    const { table, calls } = makeTable([appendRejected(), appendOk(1)]);
    const stream = table.appendStream()
      .maxRetries(1)
      .initialBackoff(0)
      .maxBackoff(0)
      .build();

    await stream.send({ id: 1 });
    assert.equal((await stream.flush())?.num_rows_inserted, 1);
    await stream.shutdown();

    assert.equal(calls.length, 2);
    assert.equal(
      requestBodyText(calls[0]!.init),
      requestBodyText(calls[1]!.init),
    );
  });

  it("safely retries an explicitly rejected append body timeout", async () => {
    const { table, calls } = makeTable([appendRejected(408), appendOk(1)]);
    const stream = table.appendStream()
      .maxRetries(1)
      .initialBackoff(0)
      .build();

    await stream.send({ id: 1 });
    assert.equal((await stream.shutdown())?.num_rows_inserted, 1);
    assert.equal(calls.length, 2);
  });

  it("honors Retry-After before safely retrying a rejected batch", async () => {
    const rejected = appendRejected(429);
    rejected.headers.set("Retry-After", "0.02");
    const { table, calls } = makeTable([rejected, appendOk(1)]);
    const stream = table.appendStream()
      .maxRetries(1)
      .initialBackoff(0)
      .maxBackoff(100)
      .build();

    await stream.send({ id: 1 });
    const startedAt = Date.now();
    assert.equal((await stream.shutdown())?.num_rows_inserted, 1);

    assert.ok(Date.now() - startedAt >= 15);
    assert.equal(calls.length, 2);
  });

  it("cancels another batch's retry delay when the stream becomes fatal", async () => {
    const { table, calls } = makeTable([appendRejected(), appendUnknown()]);
    const stream = table.appendStream()
      .batchBytes(1)
      .maxInFlightRequests(2)
      .maxRetries(1)
      .initialBackoff(1_000)
      .maxBackoff(1_000)
      .build();

    await stream.send({ id: 1 });
    await waitForCallCount(calls, 1);
    await delay(10);
    await stream.send({ id: 2 });
    await waitForCallCount(calls, 2);

    const startedAt = Date.now();
    await assert.rejects(() => stream.flush(), AppendRowsError);
    assert.ok(
      Date.now() - startedAt < 500,
      "fatal append should interrupt another batch's retry backoff",
    );
    assert.equal(calls.length, 2);
    assert.equal(stream.stats().failedRows, 1);
    assert.equal(stream.stats().unknownRows, 1);
    assert.equal(stream.stats().retries, 0);
    await assert.rejects(() => stream.shutdown(), AppendRowsError);
  });

  it("never retries an unknown commit outcome", async () => {
    const { table, calls } = makeTable([appendUnknown()]);
    const stream = table.appendStream()
      .maxRetries(8)
      .initialBackoff(0)
      .build();

    await stream.send({ id: 1 });
    await assert.rejects(
      () => stream.flush(),
      (error: unknown) => {
        assert.ok(error instanceof AppendRowsError);
        assert.equal(error.appendState, "unknown");
        assert.ok(error.isPersistent());
        return true;
      },
    );
    await assert.rejects(() => stream.shutdown(), AppendRowsError);
    assert.equal(calls.length, 1);
  });

  it("treats an invalid success response as unknown and does not retry", async () => {
    const { table, calls } = makeTable([jsonResponse(200, {})]);
    const stream = table.appendStream()
      .maxRetries(8)
      .initialBackoff(0)
      .build();

    await stream.send({ id: 1 });
    await assert.rejects(
      () => stream.flush(),
      (error: unknown) => {
        assert.ok(error instanceof AppendRowsError);
        assert.equal(error.appendState, "unknown");
        return true;
      },
    );
    assert.equal(calls.length, 1);
  });

  it("treats a transport failure as unknown and does not retry", async () => {
    const { table, calls } = makeTable([]);
    const stream = table.appendStream()
      .maxRetries(8)
      .initialBackoff(0)
      .build();

    await stream.send({ id: 1 });
    await assert.rejects(
      () => stream.flush(),
      (error: unknown) => {
        assert.ok(error instanceof AppendRowsError);
        assert.equal(error.appendState, "unknown");
        assert.ok(error.isPersistent());
        return true;
      },
    );
    assert.equal(calls.length, 1);
  });

  it("preserves the append error after exhausting safe retries", async () => {
    const finalResponse = appendRejected();
    finalResponse.headers.set("X-Request-Id", "req-retry-exhausted");
    finalResponse.headers.set("Retry-After", "0");
    const { table, calls } = makeTable([appendRejected(), finalResponse]);
    const stream = table.appendStream()
      .maxRetries(1)
      .initialBackoff(0)
      .maxBackoff(0)
      .build();

    await stream.send({ id: 1 });
    await assert.rejects(
      () => stream.flush(),
      (error: unknown) => {
        assert.ok(error instanceof AppendRowsError);
        assert.equal(error.appendState, "rejected");
        assert.ok(error.isPersistent());
        assert.equal(error.context().get("retries"), "1");
        assert.equal(error.httpStatus, 503);
        assert.equal(error.requestId, "req-retry-exhausted");
        assert.equal(error.retryAfterMs, 0);
        return true;
      },
    );
    assert.equal(calls.length, 2);
  });

  it("stops before dispatching queued batches after a fatal append", async () => {
    const { table, calls } = makeTable([appendUnknown()]);
    const stream = table.appendStream()
      .batchBytes(1)
      .maxInFlightRequests(1)
      .maxRetries(0)
      .build();

    await stream.send({ id: 1 });
    await stream.send({ id: 2 });
    await stream.send({ id: 3 });

    await assert.rejects(() => stream.flush(), AppendRowsError);
    await assert.rejects(() => stream.shutdown(), AppendRowsError);
    assert.equal(calls.length, 1);
  });
});

describe("AppendStream local failures", () => {
  it("rejects invalid numeric builder configuration", () => {
    const { table } = makeTable([]);
    const invalidConfigurations = [
      () => table.appendStream().batchBytes(Number.NaN),
      () => table.appendStream().batchBytes(8 * 1024 * 1024 + 1),
      () => table.appendStream().flushInterval(Number.POSITIVE_INFINITY),
      () => table.appendStream().channelCapacity(0),
      () => table.appendStream().maxPendingBytes(Number.NaN),
      () => table.appendStream().maxInFlightRequests(Number.POSITIVE_INFINITY),
      () => table.appendStream().maxRetries(Number.POSITIVE_INFINITY),
      () => table.appendStream().initialBackoff(-1),
      () => table.appendStream().maxBackoff(0.5),
      () => table.appendStream().attemptTimeout(0),
      () => table.appendStream().maxBatchRows(0),
      () => table.appendStream().maxBatchRows(200_001),
      () => table.appendStream().circuitBreaker({
        failureThreshold: 0,
        cooldownMs: 1_000,
      }),
      () => table.appendStream().circuitBreaker({
        failureThreshold: 1,
        cooldownMs: Number.POSITIVE_INFINITY,
      }),
      () => table.appendStream().circuitBreaker(null as never),
    ];

    for (const configure of invalidConfigurations) {
      assert.throws(
        configure,
        (error: unknown) => {
          assert.ok(error instanceof ScopeDBError);
          assert.equal(error.kind, "ConfigInvalid");
          return true;
        },
      );
    }
  });

  it("rejects a row larger than the 8 MiB stream request limit", async () => {
    const { table, calls } = makeTable([]);
    const stream = table.appendStream().build();

    await assert.rejects(
      () => stream.send({ payload: "x".repeat(8 * 1024 * 1024) }),
      (error: unknown) =>
        error instanceof ScopeDBError &&
        error.kind === "AppendRowsFailed" &&
        error.message.includes("8388608-byte append limit"),
    );
    assert.equal(await stream.shutdown(), null);
    assert.equal(calls.length, 0);
  });

  it("rejects invalid append-stream factory options at runtime", () => {
    const { table } = makeTable([]);

    for (const build of [
      () => table.appendStream({ failurePolicy: "ignore" as never }),
      () => table.appendStream({ failurePolicy: undefined as never }),
      () => table.appendStream(null as never),
    ]) {
      assert.throws(
        build,
        (error: unknown) => {
          assert.ok(error instanceof ScopeDBError);
          assert.equal(error.kind, "ConfigInvalid");
          return true;
        },
      );
    }
  });

  it("applies pending-byte backpressure before enqueueing a record", async () => {
    const { table, calls } = makeTable([]);
    const stream = table.appendStream().maxPendingBytes(1).build();

    await assert.rejects(
      () => stream.send({ id: 1 }),
      (error: unknown) => {
        assert.ok(error instanceof ScopeDBError);
        assert.ok(error.isPermanent());
        return true;
      },
    );
    await stream.shutdown();
    assert.equal(calls.length, 0);
  });

  it("keeps the stream usable after a record serialization error", async () => {
    const { table, calls } = makeTable([appendOk(1)]);
    const stream = table.appendStream().build();

    await assert.rejects(() => stream.send(undefined as never), ScopeDBError);
    await stream.send({ id: 1 });
    assert.equal((await stream.shutdown())?.num_rows_inserted, 1);
    assert.equal(calls.length, 1);
  });

  it("accepts only top-level JSON objects", async () => {
    const { table, calls } = makeTable([appendOk(1)]);
    const stream = table.appendStream().build();

    await assert.rejects(
      () => stream.send([]),
      (error: unknown) => {
        assert.ok(error instanceof ScopeDBError);
        assert.match(error.message, /JSON objects/);
        return true;
      },
    );
    await assert.rejects(
      () => stream.send(new Date()),
      (error: unknown) => {
        assert.ok(error instanceof ScopeDBError);
        assert.match(error.message, /serialize to JSON objects/);
        return true;
      },
    );

    await stream.send({ id: 1 });
    await stream.shutdown();
    assert.equal(calls.length, 1);
  });

  it("does not start shutdown when its signal is already aborted", async () => {
    const { table, calls } = makeTable([appendOk(1)]);
    const stream = table.appendStream().build();
    const controller = new AbortController();
    controller.abort();

    await assert.rejects(stream.shutdown({ signal: controller.signal }));
    assert.equal(stream.stats().state, "open");

    await stream.send({ id: 1 });
    assert.equal((await stream.shutdown())?.num_rows_inserted, 1);
    assert.equal(calls.length, 1);
  });

  it("observes a barrier that fails after its caller aborts", async () => {
    const calls: FetchCall[] = [];
    const gate = new TestDeferred<void>();
    const fetch: typeof globalThis.fetch = async (input, init) => {
      calls.push({ url: input.toString(), init });
      await gate.promise;
      return appendUnknown();
    };
    const client = new Client("http://localhost:8080", { fetch });
    const stream = client.table("events")
      .appendStream()
      .batchBytes(1)
      .build();
    await stream.send({ id: 1 });
    await waitForCallCount(calls, 1);

    const controller = new AbortController();
    const flushing = stream.flush({ signal: controller.signal });
    controller.abort();
    await assert.rejects(flushing);

    gate.resolve();
    await assert.rejects(
      () => stream.flush(),
      (error: unknown) =>
        error instanceof AppendRowsError && error.appendState === "unknown",
    );
  });
});

describe("AppendStream telemetry admission", () => {
  it("trySend is non-blocking and reports accepted, full, invalid, and closed records", async () => {
    const first = { id: 1 };
    const reservedBytes = Buffer.byteLength(JSON.stringify(first), "utf8") + 1;
    const { table, calls } = makeTable([appendOk(1)]);
    const stream = table.appendStream({ failurePolicy: "continue" })
      .batchBytes(1024)
      .flushInterval(60_000)
      .maxPendingBytes(reservedBytes)
      .build();

    assert.equal(stream.trySend(first), true);
    assert.equal(stream.trySend({ id: 2 }), false);
    assert.doesNotThrow(() => {
      assert.equal(stream.trySend(undefined as never), false);
    });

    const report = await stream.flush();
    assert.equal(report.outcome, "partial");
    assert.equal(report.acceptedRows, 1);
    assert.equal(report.committedRows, 1);
    assert.equal(report.failedRows, 0);
    assert.equal(report.unknownRows, 0);
    assert.equal(report.droppedRows, 2);

    await stream.shutdown();
    assert.equal(stream.trySend({ id: 3 }), false);

    const stats = stream.stats();
    assert.equal(stats.state, "closed");
    assert.equal(stats.acceptedRows, 1);
    assert.equal(stats.committedRows, 1);
    assert.equal(stats.failedRows, 0);
    assert.equal(stats.unknownRows, 0);
    assert.equal(stats.droppedRows, 3);
    assert.equal(stats.droppedByReason.bufferFull, 1);
    assert.equal(stats.droppedByReason.invalidRecord, 1);
    assert.equal(stats.droppedByReason.closed, 1);
    assert.equal(stats.pendingRows, 0);
    assert.equal(stats.pendingBytes, 0);
    assert.equal(calls.length, 1);
  });

  it("sendAll admits a synchronous iterable with bounded backpressure", async () => {
    const rows = [{ id: 1 }, { id: 2 }, { id: 3 }];
    const { table, calls } = makeTable([appendOk(rows.length)]);
    const stream = table.appendStream({ failurePolicy: "continue" })
      .batchBytes(1024)
      .build();

    assert.deepEqual(await stream.sendAll(rows), { acceptedRows: rows.length });
    const report = await stream.flush();
    assert.equal(report.acceptedRows, rows.length);
    assert.equal(report.committedRows, rows.length);
    assert.deepEqual(parseAppendRows(calls[0]!), rows);
    await stream.shutdown();
  });

  it("sendAll admits an async iterable in iteration order", async () => {
    async function* rows() {
      yield { id: 1 };
      await delay(0);
      yield { id: 2 };
      yield { id: 3 };
    }

    const { table, calls } = makeTable([appendOk(3)]);
    const stream = table.appendStream({ failurePolicy: "continue" })
      .batchBytes(1024)
      .build();

    assert.deepEqual(await stream.sendAll(rows()), { acceptedRows: 3 });
    const report = await stream.shutdown();

    assert.equal(report.acceptedRows, 3);
    assert.equal(report.committedRows, 3);
    assert.deepEqual(parseAppendRows(calls[0]!), [
      { id: 1 },
      { id: 2 },
      { id: 3 },
    ]);
  });

  it("sendAll honors AbortSignal while waiting for local capacity", async () => {
    const row = { id: 0 };
    const reservedBytes = Buffer.byteLength(JSON.stringify(row), "utf8") + 1;
    const { table, calls } = makeTable([appendOk(1)]);
    const stream = table.appendStream({ failurePolicy: "continue" })
      .batchBytes(1024)
      .flushInterval(60_000)
      .maxPendingBytes(reservedBytes)
      .build();

    await stream.send(row);
    const controller = new AbortController();
    const sending = stream.sendAll([{ id: 1 }, { id: 2 }], {
      signal: controller.signal,
    });
    await delay(0);
    controller.abort();

    await assert.rejects(sending);
    const report = await stream.flush();
    assert.equal(report.acceptedRows, 1);
    assert.equal(report.committedRows, 1);
    assert.deepEqual(parseAppendRows(calls[0]!), [row]);
    await stream.shutdown();
  });
});

describe("AppendStream best-effort delivery", () => {
  it("selects typed strict and continue failure behavior at the factory", async () => {
    const { table } = makeTable([appendRejected(400), appendRejected(400)]);
    const strict: AppendStream<"stop"> = table.appendStream()
      .batchBytes(1)
      .maxRetries(0)
      .build();
    const bestEffort: AppendStream<"continue"> = table
      .appendStream({ failurePolicy: "continue" })
      .batchBytes(1)
      .maxRetries(0)
      .build();

    await strict.send({ id: 1 });
    await assert.rejects(() => strict.flush(), AppendRowsError);

    await bestEffort.send({ id: 2 });
    const report = await bestEffort.flush();
    assert.equal(report.outcome, "failed");
    assert.equal(report.failedRows, 1);
    await bestEffort.shutdown();
  });

  it("accepts explicit undefined and optional factory options", async () => {
    const { table } = makeTable([]);
    const strict: AppendStream<"stop"> = table.appendStream(undefined).build();
    const buildOptional = (
      options?: AppendStreamOptions<"continue">,
    ): AppendStream<"stop" | "continue"> => table.appendStream(options).build();
    const optional = buildOptional(undefined);

    assert.equal(await strict.shutdown(), null);
    assert.equal(await optional.shutdown(), null);
  });

  it("continues after a permanently rejected batch and reports the batch failure", async () => {
    const errors: ScopeDBError[] = [];
    const { table, calls } = makeTable([appendRejected(400), appendOk(1)]);
    const stream = table.appendStream({ failurePolicy: "continue" })
      .onBatchFailure(({ error }) => {
        errors.push(error);
      })
      .batchBytes(1)
      .maxInFlightRequests(1)
      .maxRetries(0)
      .build();

    await stream.send({ id: 1 });
    await waitForCallCount(calls, 1);
    await stream.send({ id: 2 });
    const report = await stream.flush();

    assert.equal(report.outcome, "partial");
    assert.equal(report.acceptedRows, 2);
    assert.equal(report.committedRows, 1);
    assert.equal(report.failedRows, 1);
    assert.equal(report.unknownRows, 0);
    assert.equal(errors.length, 1);
    assert.ok(errors[0] instanceof AppendRowsError);
    assert.equal((errors[0] as AppendRowsError).appendState, "rejected");
    assert.equal(stream.stats().lastFailure?.appendState, "rejected");
    assert.equal(typeof stream.stats().lastFailure?.atMs, "number");
    assert.deepEqual(calls.map(parseAppendRows), [[{ id: 1 }], [{ id: 2 }]]);
    await stream.shutdown();
  });

  it("isolates synchronous and asynchronous batch failure listener errors", async () => {
    const unhandledRejections: unknown[] = [];
    const onUnhandledRejection = (reason: unknown) => {
      unhandledRejections.push(reason);
    };
    process.on("unhandledRejection", onUnhandledRejection);

    const { table } = makeTable([appendRejected(400)]);
    let observed = 0;
    const stream = table.appendStream({ failurePolicy: "continue" })
      .onBatchFailure(() => {
        throw new Error("synchronous listener failure");
      })
      .onBatchFailure(async () => {
        throw new Error("asynchronous listener failure");
      })
      .onBatchFailure(() => {
        observed += 1;
      })
      .maxRetries(0)
      .build();

    try {
      await stream.send({ id: 1 });
      const report = await stream.flush();
      await delay(0);

      assert.equal(report.outcome, "failed");
      assert.equal(observed, 1);
      assert.deepEqual(unhandledRejections, []);
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
      await stream.shutdown();
    }
  });

  it("does not retry an unknown batch but continues with a new batch", async () => {
    const errors: ScopeDBError[] = [];
    const { table, calls } = makeTable([appendUnknown(), appendOk(1)]);
    const stream = table.appendStream({ failurePolicy: "continue" })
      .onBatchFailure(({ error }) => {
        errors.push(error);
      })
      .batchBytes(1)
      .maxInFlightRequests(1)
      .maxRetries(8)
      .initialBackoff(0)
      .build();

    await stream.send({ id: 1 });
    await waitForCallCount(calls, 1);
    await stream.send({ id: 2 });
    const report = await stream.flush();

    assert.equal(report.outcome, "partial");
    assert.equal(report.acceptedRows, 2);
    assert.equal(report.committedRows, 1);
    assert.equal(report.failedRows, 0);
    assert.equal(report.unknownRows, 1);
    assert.equal(errors.length, 1);
    assert.ok(errors[0] instanceof AppendRowsError);
    assert.equal((errors[0] as AppendRowsError).appendState, "unknown");
    assert.equal(calls.length, 2);
    assert.deepEqual(calls.map(parseAppendRows), [[{ id: 1 }], [{ id: 2 }]]);
    await stream.shutdown();
  });

  it("classifies an attempt timeout as unknown and emits a batch failure", async () => {
    const calls: FetchCall[] = [];
    const errors: ScopeDBError[] = [];
    let receivedSignal = false;
    const fetch: typeof globalThis.fetch = (input, init) => {
      calls.push({ url: input.toString(), init });
      return new Promise<Response>((_resolve, reject) => {
        const fallback = setTimeout(
          () => reject(new Error("append request did not receive a timeout signal")),
          250,
        );
        const signal = init?.signal;
        if (signal === undefined || signal === null) {
          return;
        }
        receivedSignal = true;
        const rejectAborted = () => {
          clearTimeout(fallback);
          reject(signal.reason);
        };
        if (signal.aborted) {
          rejectAborted();
        } else {
          signal.addEventListener("abort", rejectAborted, { once: true });
        }
      });
    };
    const client = new Client("http://localhost:8080", { fetch });
    const stream = client.table("events")
      .appendStream({ failurePolicy: "continue" })
      .onBatchFailure(({ error }) => {
        errors.push(error);
      })
      .attemptTimeout(10)
      .batchBytes(1)
      .maxRetries(0)
      .build();

    await stream.send({ id: 1 });
    const startedAt = Date.now();
    const report = await stream.flush();

    assert.equal(receivedSignal, true);
    assert.ok(Date.now() - startedAt < 200, "attempt timeout should bound the append");
    assert.equal(report.outcome, "unknown");
    assert.equal(report.acceptedRows, 1);
    assert.equal(report.committedRows, 0);
    assert.equal(report.failedRows, 0);
    assert.equal(report.unknownRows, 1);
    assert.equal(calls.length, 1);
    assert.equal(errors.length, 1);
    assert.ok(errors[0] instanceof AppendRowsError);
    assert.equal((errors[0] as AppendRowsError).appendState, "unknown");
    await stream.shutdown();
  });

  it("returns interval flush reports that distinguish committed, failed, and unknown rows", async () => {
    const { table, calls } = makeTable([
      appendOk(1),
      appendRejected(400),
      appendUnknown(),
    ]);
    const stream = table.appendStream({ failurePolicy: "continue" })
      .batchBytes(1)
      .maxInFlightRequests(1)
      .maxRetries(0)
      .build();

    await stream.send({ id: 1 });
    await stream.send({ id: 2 });
    await stream.send({ id: 3 });
    const report = await stream.flush();

    assert.equal(report.outcome, "partial");
    assert.equal(report.acceptedRows, 3);
    assert.equal(report.committedRows, 1);
    assert.equal(report.failedRows, 1);
    assert.equal(report.unknownRows, 1);
    assert.equal(report.droppedRows, 0);
    assert.equal(report.committedBatches, 1);
    assert.equal(report.failedBatches, 1);
    assert.equal(report.unknownBatches, 1);
    assert.equal(report.retries, 0);
    assert.equal(calls.length, 3);

    const empty = await stream.flush();
    assert.equal(empty.outcome, "ok");
    assert.equal(empty.acceptedRows, 0);
    assert.equal(empty.committedRows, 0);
    assert.equal(empty.failedRows, 0);
    assert.equal(empty.unknownRows, 0);
    assert.equal(empty.droppedRows, 0);
    await stream.shutdown();
  });

  it("snapshots local drops when a barrier is requested", async () => {
    const calls: FetchCall[] = [];
    const gate = new TestDeferred<void>();
    const fetch: typeof globalThis.fetch = async (input, init) => {
      calls.push({ url: input.toString(), init });
      await gate.promise;
      return appendOk(1);
    };
    const client = new Client("http://localhost:8080", { fetch });
    const stream = client.table("events")
      .appendStream({ failurePolicy: "continue" })
      .batchBytes(1)
      .build();

    await stream.send({ id: 1 });
    await waitForCallCount(calls, 1);
    const flushing = stream.flush();
    assert.equal(stream.trySend(undefined as never), false);
    gate.resolve();

    const first = await flushing;
    assert.equal(first.outcome, "ok");
    assert.equal(first.droppedRows, 0);

    const second = await stream.flush();
    assert.equal(second.outcome, "failed");
    assert.equal(second.droppedRows, 1);
    assert.deepEqual(stream.stats().lastReport, second);
    await stream.shutdown();
  });

  it("opens the circuit after repeated availability failures and probes after cooldown", async () => {
    const { table, calls } = makeTable([appendUnknown(), appendOk(1)]);
    const stream = table.appendStream({ failurePolicy: "continue" })
      .circuitBreaker({ failureThreshold: 1, cooldownMs: 20 })
      .batchBytes(1)
      .maxInFlightRequests(1)
      .maxRetries(0)
      .build();

    await stream.send({ id: 1 });
    await waitForCallCount(calls, 1);
    await stream.send({ id: 2 });
    const flushing = stream.flush();

    await delay(5);
    assert.equal(calls.length, 1);
    assert.equal(stream.stats().circuitState, "open");
    assert.equal(stream.trySend({ id: 3 }), false);
    assert.equal(stream.stats().droppedByReason.circuitOpen, 1);

    const report = await flushing;
    assert.equal(report.outcome, "partial");
    assert.equal(report.unknownRows, 1);
    assert.equal(report.committedRows, 1);
    assert.equal(stream.stats().circuitState, "closed");
    assert.equal(calls.length, 2);
    await stream.shutdown();
  });

  it("allows one trySend probe after an idle circuit cooldown", async () => {
    const { table, calls } = makeTable([appendUnknown(), appendOk(1)]);
    const stream = table.appendStream({ failurePolicy: "continue" })
      .circuitBreaker({ failureThreshold: 1, cooldownMs: 10 })
      .batchBytes(1)
      .maxRetries(0)
      .build();

    assert.equal(stream.trySend({ id: 1 }), true);
    const failed = await stream.flush();
    assert.equal(failed.unknownRows, 1);
    assert.equal(stream.stats().circuitState, "open");
    assert.equal(stream.trySend({ id: 2 }), false);

    await delay(15);
    assert.equal(stream.trySend({ id: 3 }), true);
    assert.equal(stream.trySend({ id: 4 }), false);
    const recovered = await stream.flush();

    assert.equal(recovered.committedRows, 1);
    assert.equal(recovered.droppedRows, 2);
    assert.equal(stream.stats().circuitState, "closed");
    assert.equal(calls.length, 2);
    await stream.shutdown();
  });

  it("settles pre-open requests before allowing one half-open probe", async () => {
    const calls: FetchCall[] = [];
    const firstFailure = new TestDeferred<void>();
    const lateOldFailure = new TestDeferred<void>();
    const probe = new TestDeferred<void>();
    let callIndex = 0;
    const fetch: typeof globalThis.fetch = async (input, init) => {
      const index = callIndex++;
      calls.push({ url: input.toString(), init });
      if (index === 0) {
        await firstFailure.promise;
        return appendUnknown();
      }
      if (index === 1) {
        await lateOldFailure.promise;
        return appendUnknown();
      }
      if (index === 2) {
        await probe.promise;
      }
      return appendOk(1);
    };
    const client = new Client("http://localhost:8080", { fetch });
    const stream = client.table("events")
      .appendStream({ failurePolicy: "continue" })
      .circuitBreaker({ failureThreshold: 1, cooldownMs: 10 })
      .batchBytes(1)
      .maxInFlightRequests(2)
      .maxRetries(0)
      .build();

    await stream.send({ id: 1 });
    await stream.send({ id: 2 });
    await stream.send({ id: 3 });
    await stream.send({ id: 4 });
    await waitForCallCount(calls, 2);
    const flushing = stream.flush();

    firstFailure.resolve();
    await delay(20);
    assert.equal(calls.length, 2, "old in-flight request must settle first");

    lateOldFailure.resolve();
    await waitForCallCount(calls, 3);
    await delay(20);
    assert.equal(calls.length, 3, "only one half-open probe may run");

    probe.resolve();
    await waitForCallCount(calls, 4);
    const report = await flushing;
    assert.equal(report.committedRows, 2);
    assert.equal(report.unknownRows, 2);
    assert.equal(stream.stats().circuitState, "closed");
    await stream.shutdown();
  });

  it("treats a committed response with the wrong row count as unknown", async () => {
    const { table, calls } = makeTable([appendOk(0)]);
    const stream = table.appendStream({ failurePolicy: "continue" })
      .batchBytes(1)
      .maxRetries(8)
      .build();

    await stream.send({ id: 1 });
    const report = await stream.flush();

    assert.equal(report.outcome, "unknown");
    assert.equal(report.committedRows, 0);
    assert.equal(report.unknownRows, 1);
    assert.equal(calls.length, 1);
    await stream.shutdown();
  });

  it("shutdown is one idempotent barrier and returns its delivery report", async () => {
    const { table, calls } = makeTable([appendOk(1)]);
    const stream = table.appendStream({ failurePolicy: "continue" })
      .build();
    assert.equal(stream.trySend({ id: 1 }), true);

    const first = stream.shutdown();
    const second = stream.shutdown();

    assert.equal(first, second);
    const report = await first;
    assert.equal(report.outcome, "ok");
    assert.equal(report.acceptedRows, 1);
    assert.equal(report.committedRows, 1);
    assert.equal(report.failedRows, 0);
    assert.equal(report.unknownRows, 0);
    assert.equal(calls.length, 1);
    assert.equal(stream.stats().state, "closed");
  });
});

class TestDeferred<T> {
  readonly promise: Promise<T>;
  resolve!: (value: T) => void;

  constructor() {
    this.promise = new Promise<T>((resolve) => {
      this.resolve = resolve;
    });
  }
}

async function waitForCallCount(calls: FetchCall[], expected: number): Promise<void> {
  const deadline = Date.now() + 1_000;
  while (calls.length < expected) {
    if (Date.now() >= deadline) {
      assert.fail(`timed out waiting for ${expected} fetch call(s), got ${calls.length}`);
    }
    await delay(1);
  }
}

async function delay(ms: number): Promise<void> {
  await new Promise<void>((resolve) => setTimeout(resolve, ms));
}
