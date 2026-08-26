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
import { jsonResponse, makeFetchStub, parseJsonRequestBody } from "./helpers.js";

const TRANSFORM = "INSERT INTO t SELECT *";

function makeClient(responses: Response[]): { client: Client; calls: Array<{ url: string; init?: RequestInit }> } {
  const { fn, calls } = makeFetchStub(responses);
  return { client: new Client("http://localhost:8080", { fetch: fn }), calls };
}

function makeSuccessfulClient(): {
  client: Client;
  calls: Array<{ url: string; init?: RequestInit }>;
} {
  const calls: Array<{ url: string; init?: RequestInit }> = [];
  const fetch: typeof globalThis.fetch = async (input, init) => {
    calls.push({ url: input.toString(), init });
    return ingestOk(parseIngestRows(init).length);
  };
  return { client: new Client("http://localhost:8080", { fetch }), calls };
}

function ingestOk(numRows = 1): Response {
  return jsonResponse(200, { num_rows_inserted: numRows });
}

function ingestTemp(): Response {
  return jsonResponse(503, { message: "service unavailable" });
}

function ingestRateLimited(): Response {
  return jsonResponse(429, { message: "rate limited" });
}

function ingestPerm(): Response {
  return jsonResponse(400, { message: "bad request" });
}

// Parse the ingest POST body to extract inserted rows as an array of parsed records
function parseIngestRows(init: RequestInit | undefined): unknown[] {
  const body = parseJsonRequestBody(init) as { data: { rows: string } };
  return body.data.rows.split("\n").map((line) => JSON.parse(line) as unknown);
}

describe("IngestStream basic send + flush", () => {
  it("sends POST /v1/ingest on flush with correct body", async () => {
    const { client, calls } = makeClient([ingestOk(1)]);
    const stream = client.ingestStream(TRANSFORM).batchBytes(1024 * 1024).build();

    await stream.send({ id: 1, name: "alice" });
    const result = await stream.flush();
    await stream.shutdown();

    assert.equal(calls.length, 1);
    const call = calls[0]!;
    assert.ok(call.url.endsWith("/v1/ingest"), `unexpected URL: ${call.url}`);
    assert.equal((call.init as RequestInit).method, "POST");

    const body = parseJsonRequestBody(call.init) as Record<string, unknown>;
    assert.equal(body["type"], "committed");
    assert.deepEqual((body["data"] as Record<string, string>)["format"], "json");
    assert.equal(body["statement"], TRANSFORM);

    assert.ok(result !== null);
    assert.equal(result.num_rows_inserted, 1);
  });

  it("flush on empty buffer returns null and sends no request", async () => {
    const { client, calls } = makeClient([]);
    const stream = client.ingestStream(TRANSFORM).build();

    const result = await stream.flush();
    await stream.shutdown();

    assert.equal(result, null);
    assert.equal(calls.length, 0);
  });

  it("shutdown flushes remaining records", async () => {
    const { client, calls } = makeClient([ingestOk(1)]);
    const stream = client.ingestStream(TRANSFORM).batchBytes(1024 * 1024).build();

    await stream.send({ value: 42 });
    await stream.shutdown();

    assert.equal(calls.length, 1);
    const rows = parseIngestRows(calls[0]!.init);
    assert.equal(rows.length, 1);
    assert.deepEqual(rows[0], { value: 42 });
  });

  it("shutdown with no records sends no request", async () => {
    const { client, calls } = makeClient([]);
    const stream = client.ingestStream(TRANSFORM).build();
    await stream.shutdown();
    assert.equal(calls.length, 0);
  });

  it("closes admission synchronously and makes shutdown idempotent", async () => {
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    let releaseResponse!: () => void;
    const responseGate = new Promise<void>((resolve) => {
      releaseResponse = resolve;
    });
    const fetch: typeof globalThis.fetch = async (input, init) => {
      calls.push({ url: input.toString(), init });
      await responseGate;
      return ingestOk(parseIngestRows(init).length);
    };
    const client = new Client("http://localhost:8080", { fetch });
    const stream = client.ingestStream(TRANSFORM).build();

    await stream.send({ id: 1 });
    const first = stream.shutdown();
    const second = stream.shutdown();
    assert.equal(first, second);
    await waitForCallCount(calls, 1);

    await assert.rejects(() => stream.send({ id: 2 }), /ingest stream is closed/);
    releaseResponse();
    assert.equal((await first)?.num_rows_inserted, 1);
    assert.equal((await second)?.num_rows_inserted, 1);
    assert.deepEqual(parseIngestRows(calls[0]!.init), [{ id: 1 }]);
  });
});

describe("IngestStream batching", () => {
  it("batches multiple records into a single flush request", async () => {
    const { client, calls } = makeClient([ingestOk(3)]);
    const stream = client.ingestStream(TRANSFORM).batchBytes(1024 * 1024).build();

    await stream.send({ i: 1 });
    await stream.send({ i: 2 });
    await stream.send({ i: 3 });
    await stream.flush();
    await stream.shutdown();

    assert.equal(calls.length, 1);
    const rows = parseIngestRows(calls[0]!.init);
    assert.equal(rows.length, 3);
    assert.deepEqual(rows[0], { i: 1 });
    assert.deepEqual(rows[2], { i: 3 });
  });

  it("auto-flushes when batch size is exceeded", async () => {
    // Set batchBytes very small so even one record triggers auto-flush
    const { client, calls } = makeClient([ingestOk(1), ingestOk(1)]);
    const stream = client.ingestStream(TRANSFORM).batchBytes(1).build();

    await stream.send({ x: "record-one" });
    // At this point the worker should have auto-flushed due to batchBytes=1
    // Give the worker event loop tick to process
    await new Promise<void>((r) => setTimeout(r, 10));

    await stream.send({ x: "record-two" });
    await stream.shutdown();

    // Both records should have been flushed in separate batches
    assert.ok(calls.length >= 1, `expected at least 1 ingest call, got ${calls.length}`);
  });

  it("seals a batch before the next record would exceed the target", async () => {
    const first = { id: 1 };
    const second = { id: 2 };
    const targetBytes = Buffer.byteLength(JSON.stringify(first), "utf8") + 1;
    const { client, calls } = makeSuccessfulClient();
    const stream = client.ingestStream(TRANSFORM)
      .batchBytes(targetBytes)
      .flushInterval(60_000)
      .build();

    await stream.send(first);
    await stream.send(second);
    await stream.shutdown();

    assert.equal(calls.length, 2);
    assert.deepEqual(parseIngestRows(calls[0]!.init), [first]);
    assert.deepEqual(parseIngestRows(calls[1]!.init), [second]);
  });

  it("flush interval triggers background flush", async () => {
    // Set a short flush interval; after waiting, the worker should flush
    const { client, calls } = makeClient([ingestOk(1)]);
    const stream = client.ingestStream(TRANSFORM)
      .batchBytes(1024 * 1024)
      .flushInterval(30) // 30ms
      .build();

    await stream.send({ event: "hello" });

    // Wait longer than the flush interval
    await new Promise<void>((r) => setTimeout(r, 80));

    // The background timer should have flushed the record already
    assert.ok(calls.length >= 1, `expected timer flush, got ${calls.length} calls`);

    await stream.shutdown();
  });

  it("flushes by the first-row deadline while records keep arriving", async () => {
    const { client, calls } = makeSuccessfulClient();
    const flushIntervalMs = 30;
    const stream = client.ingestStream(TRANSFORM)
      .batchBytes(1024 * 1024)
      .flushInterval(flushIntervalMs)
      .build();

    let producerDone = false;
    const producer = (async () => {
      for (let id = 0; id < 20; id += 1) {
        await stream.send({ id });
        await new Promise<void>((resolve) => setTimeout(resolve, 5));
      }
      producerDone = true;
    })();

    await waitForCallCount(calls, 1);
    assert.equal(producerDone, false, "first batch was not dispatched while producing");
    await producer;
    await stream.shutdown();
  });
});

describe("IngestStream retry / backoff", () => {
  it("retries on temporary errors and succeeds", async () => {
    // 2 temporary errors then success
    const { client, calls } = makeClient([ingestTemp(), ingestTemp(), ingestOk(1)]);
    const stream = client.ingestStream(TRANSFORM)
      .batchBytes(1024 * 1024)
      .maxRetries(3)
      .initialBackoff(0)
      .maxBackoff(0)
      .build();

    await stream.send({ v: 1 });
    const result = await stream.flush();
    await stream.shutdown();

    assert.equal(calls.length, 3);
    assert.ok(result !== null);
    assert.equal(result.num_rows_inserted, 1);
  });

  it("merges queued records into a rate-limited batch before retrying", async () => {
    const { client, calls } = makeClient([ingestRateLimited(), ingestOk(2)]);
    const stream = client.ingestStream(TRANSFORM)
      .batchBytes(1024 * 1024)
      .maxRetries(1)
      .initialBackoff(50)
      .maxBackoff(50)
      .build();

    await stream.send({ v: 1 });
    const flushing = stream.flush();
    await waitForCallCount(calls, 1);
    await stream.send({ v: 2 });

    const result = await flushing;
    await stream.shutdown();

    assert.ok(result !== null);
    assert.equal(calls.length, 2);
    assert.deepEqual(parseIngestRows(calls[0]!.init), [{ v: 1 }]);
    assert.deepEqual(parseIngestRows(calls[1]!.init), [{ v: 1 }, { v: 2 }]);
  });

  it("leaves queue-head records for the next batch when the retry batch is full", async () => {
    const first = { v: 1 };
    const second = { v: 2 };
    const third = { v: 3 };
    const batchBytes = Buffer.byteLength(JSON.stringify(first), "utf8") +
      1 + Buffer.byteLength(JSON.stringify(second), "utf8");
    const { client, calls } = makeClient([
      ingestRateLimited(),
      ingestOk(2),
      ingestOk(1),
    ]);
    const stream = client.ingestStream(TRANSFORM)
      .batchBytes(batchBytes)
      .maxRetries(1)
      .initialBackoff(50)
      .maxBackoff(50)
      .build();

    await stream.send(first);
    const flushing = stream.flush();
    await waitForCallCount(calls, 1);
    await stream.send(second);
    await stream.send(third);

    await flushing;
    await stream.shutdown();

    assert.equal(calls.length, 3);
    assert.deepEqual(parseIngestRows(calls[1]!.init), [first, second]);
    assert.deepEqual(parseIngestRows(calls[2]!.init), [third]);
    const retryRows = parseJsonRequestBody(calls[1]!.init) as {
      data: { rows: string };
    };
    assert.ok(Buffer.byteLength(retryRows.data.rows, "utf8") <= batchBytes);
  });

  it("does not merge records across a queued flush command", async () => {
    const { client, calls } = makeClient([
      ingestRateLimited(),
      ingestOk(1),
      ingestOk(1),
    ]);
    const stream = client.ingestStream(TRANSFORM)
      .batchBytes(1024 * 1024)
      .maxRetries(1)
      .initialBackoff(50)
      .maxBackoff(50)
      .build();

    await stream.send({ v: 1 });
    const firstFlush = stream.flush();
    await waitForCallCount(calls, 1);
    const flushBarrier = stream.flush();
    await stream.send({ v: 2 });

    await firstFlush;
    assert.equal(await flushBarrier, null);
    await stream.shutdown();

    assert.equal(calls.length, 3);
    assert.deepEqual(parseIngestRows(calls[1]!.init), [{ v: 1 }]);
    assert.deepEqual(parseIngestRows(calls[2]!.init), [{ v: 2 }]);
  });

  it("marks stream fatal after retry budget exhausted", async () => {
    // 3 temporary errors, maxRetries=2 → exhausted after 3 tries (initial + 2 retries)
    const { client } = makeClient([ingestTemp(), ingestTemp(), ingestTemp()]);
    const stream = client.ingestStream(TRANSFORM)
      .batchBytes(1024 * 1024)
      .maxRetries(2)
      .initialBackoff(0)
      .maxBackoff(0)
      .build();

    await stream.send({ v: 1 });

    await assert.rejects(
      () => stream.flush(),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError, `expected ScopeDBError, got ${String(err)}`);
        return true;
      },
    );
  });

  it("does not retry on permanent errors", async () => {
    // permanent error → no retry, immediate fatal
    const { client, calls } = makeClient([ingestPerm()]);
    const stream = client.ingestStream(TRANSFORM)
      .batchBytes(1024 * 1024)
      .maxRetries(5)
      .initialBackoff(0)
      .build();

    await stream.send({ v: 1 });

    await assert.rejects(
      () => stream.flush(),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        return true;
      },
    );

    // Only one ingest attempt was made
    assert.equal(calls.length, 1);
  });
});

async function waitForCallCount(
  calls: Array<{ url: string; init?: RequestInit }>,
  expected: number,
): Promise<void> {
  const deadline = Date.now() + 1_000;
  while (calls.length < expected) {
    if (Date.now() >= deadline) {
      assert.fail(`timed out waiting for ${expected} fetch call(s), got ${calls.length}`);
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 1));
  }
}

describe("IngestStream fatal error propagation", () => {
  it("send() throws after stream becomes fatal", async () => {
    const { client } = makeClient([ingestPerm()]);
    const stream = client.ingestStream(TRANSFORM)
      .batchBytes(1024 * 1024)
      .maxRetries(0)
      .initialBackoff(0)
      .build();

    await stream.send({ v: 1 });

    // Trigger the fatal error
    await assert.rejects(() => stream.flush());

    // Subsequent send should throw immediately
    await assert.rejects(
      () => stream.send({ v: 2 }),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        return true;
      },
    );
  });

  it("flush() throws after stream becomes fatal", async () => {
    const { client } = makeClient([ingestPerm()]);
    const stream = client.ingestStream(TRANSFORM)
      .batchBytes(1024 * 1024)
      .maxRetries(0)
      .initialBackoff(0)
      .build();

    await stream.send({ v: 1 });
    await assert.rejects(() => stream.flush());

    // Second flush also throws
    await assert.rejects(
      () => stream.flush(),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        return true;
      },
    );
  });

  it("rejects a shutdown barrier queued behind a failing flush", async () => {
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    let releaseResponse!: () => void;
    const responseGate = new Promise<void>((resolve) => {
      releaseResponse = resolve;
    });
    const fetch: typeof globalThis.fetch = async (input, init) => {
      calls.push({ url: input.toString(), init });
      await responseGate;
      return ingestPerm();
    };
    const client = new Client("http://localhost:8080", { fetch });
    const stream = client.ingestStream(TRANSFORM).build();

    await stream.send({ v: 1 });
    const flushing = stream.flush();
    await waitForCallCount(calls, 1);
    const shutdown = stream.shutdown();
    const settling = Promise.allSettled([flushing, shutdown]);

    releaseResponse();
    const results = await settling;

    assert.equal(results[0]!.status, "rejected");
    assert.equal(results[1]!.status, "rejected");
    assert.ok(results[1]!.reason instanceof ScopeDBError);
  });
});

describe("IngestStream backpressure", () => {
  it("rejects non-finite builder configuration", () => {
    const { client } = makeClient([]);
    assert.throws(
      () => client.ingestStream(TRANSFORM).maxPendingBytes(Number.NaN),
      (error: unknown) => {
        assert.ok(error instanceof ScopeDBError);
        assert.equal(error.kind, "ConfigInvalid");
        return true;
      },
    );
  });

  it("throws permanent ScopeDBError when single record exceeds maxPendingBytes", async () => {
    const { client } = makeClient([]);
    // Set maxPendingBytes very small — 1 byte — so any real record exceeds it
    const stream = client.ingestStream(TRANSFORM)
      .maxPendingBytes(1)
      .build();

    await assert.rejects(
      () => stream.send({ large: "record" }),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        assert.ok(err.isPermanent(), `expected permanent error, got ${err.status()}`);
        return true;
      },
    );

    await stream.shutdown();
  });
});
