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
import { Client, ScopeDBError } from "../src/index.js";
import { jsonResponse, parseJsonRequestBody } from "./helpers.js";

const transform = "SELECT $0 INSERT INTO events";
const writes = {
  insert: (client: Client) => client.insert('{"id":1}', transform),
  ingest: (client: Client) => client.ingest({
    type: "committed",
    data: { format: "json", rows: '{"id":1}' },
    statement: transform,
  }),
};

function unknownOutcome(error: unknown): true {
  assert.ok(error instanceof ScopeDBError);
  assert.equal(error.retryable, false);
  assert.match(error.message, /commit outcome is unknown/);
  return true;
}

function interruptedBody(): Response {
  return new Response(new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new TextEncoder().encode('{"num_rows_inserted":'));
      controller.error(new TypeError("connection reset during response"));
    },
  }), { status: 200, headers: { "X-Request-ID": "interrupted-body" } });
}

for (const [name, write] of Object.entries(writes)) {
  it(`${name} treats connection loss after sending rows as an unknown outcome`, async () => {
    const cause = new TypeError("connection reset after request write");
    let requests = 0;
    const client = new Client("https://example.com", {
      fetch: async (_input, init) => {
        requests++;
        const request = parseJsonRequestBody(init) as { data: { rows: string } };
        assert.equal(request.data.rows, '{"id":1}');
        throw cause;
      },
    });
    await assert.rejects(write(client), (error: unknown) => {
      unknownOutcome(error);
      assert.ok(error instanceof ScopeDBError);
      assert.ok(error.cause instanceof ScopeDBError);
      assert.equal(error.cause.cause, cause);
      return true;
    });
    assert.equal(requests, 1);
  });

  it(`${name} preserves response metadata when the body is interrupted`, async () => {
    const client = new Client("https://example.com", { fetch: async () => interruptedBody() });
    await assert.rejects(write(client), (error: unknown) => {
      unknownOutcome(error);
      assert.ok(error instanceof ScopeDBError);
      assert.equal(error.httpStatus, 200);
      assert.equal(error.requestId, "interrupted-body");
      return true;
    });
  });

  it(`${name} rejects malformed successful responses without claiming a commit`, async () => {
    for (const body of ["{", "null", "{}", '{"num_rows_inserted":null}',
      '{"num_rows_inserted":"1"}', '{"num_rows_inserted":-1}']) {
      const client = new Client("https://example.com", {
        fetch: async () => new Response(body, { status: 200, headers: { "X-Request-ID": "invalid-body" } }),
      });
      await assert.rejects(write(client), (error: unknown) => {
        unknownOutcome(error);
        assert.ok(error instanceof ScopeDBError);
        assert.equal(error.httpStatus, 200);
        assert.equal(error.requestId, "invalid-body");
        return true;
      });
    }
  });

  it(`${name} preserves HTTP errors but disables unsafe retry hints`, async () => {
    for (const status of [400, 408, 429, 500, 503]) {
      const client = new Client("https://example.com", {
        fetch: async () => new Response(JSON.stringify({
          message: "ingest unavailable", request_id: "server-request", retryable: true,
        }), { status, headers: { "X-Request-ID": "header-request", "Retry-After": "2" } }),
      });
      await assert.rejects(write(client), (error: unknown) => {
        assert.ok(error instanceof ScopeDBError);
        assert.equal(error.message, "ingest unavailable");
        assert.equal(error.retryable, false);
        assert.equal(error.httpStatus, status);
        assert.equal(error.requestId, "server-request");
        assert.equal(error.retryAfterMs, 2_000);
        return true;
      });
    }
  });

  it(`${name} accepts nonnegative counts changed by a transform`, async () => {
    for (const numRows of [0, 3]) {
      const client = new Client("https://example.com", {
        fetch: async () => jsonResponse(200, { num_rows_inserted: numRows }),
      });
      assert.equal((await write(client)).num_rows_inserted, numRows);
    }
  });
}

for (const [name, failure] of Object.entries({
  "connection loss": () => { throw new TypeError("connection reset after request write"); },
  "interrupted body": interruptedBody,
  "invalid success": () => jsonResponse(200, { num_rows_inserted: -1 }),
})) {
  it(`ingest streams stop after ${name} without replaying accepted rows`, async () => {
    let requests = 0;
    const client = new Client("https://example.com", {
      fetch: async () => {
        requests++;
        return failure();
      },
    });
    const stream = client.ingestStream(transform)
      .maxRetries(5).initialBackoff(0).maxBackoff(0).build();
    await stream.send({ id: 1 });
    await assert.rejects(stream.flush(), unknownOutcome);
    await assert.rejects(stream.shutdown(), unknownOutcome);
    assert.equal(requests, 1);
  });
}

it("ingest streams retain HTTP failure diagnostics in subsequent operations", async () => {
  let requests = 0;
  const client = new Client("https://example.com", {
    fetch: async () => {
      requests++;
      return new Response(JSON.stringify({
        message: "ingest unavailable", request_id: "failed-ingest", retryable: true,
      }), { status: 503, headers: { "Retry-After": "2" } });
    },
  });
  const stream = client.ingestStream(transform).build();
  await stream.send({ id: 1 });
  for (const operation of [
    () => stream.flush(),
    () => stream.flush(),
    () => stream.send({ id: 2 }),
    () => stream.shutdown(),
  ]) {
    await assert.rejects(operation(), (error: unknown) => {
      assert.ok(error instanceof ScopeDBError);
      assert.equal(error.message, "ingest unavailable");
      assert.equal(error.httpStatus, 503);
      assert.equal(error.requestId, "failed-ingest");
      assert.equal(error.retryAfterMs, 2_000);
      assert.equal(error.retryable, false);
      return true;
    });
  }
  assert.equal(requests, 1);
});
