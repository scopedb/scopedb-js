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
import { Client, ScopeDBError } from "../src/index.js";
import {
  finishedStatus,
  jsonResponse,
  makeFetchStub,
  pendingStatus,
  textResponse,
} from "./helpers.js";

describe("Client configuration and transport", () => {
  it("rejects invalid endpoints and empty API keys", () => {
    for (const invalid of ["", "not-a-url", "http://"]) {
      assert.throws(
        () => new Client(invalid),
        (err: unknown) => err instanceof ScopeDBError && err.kind === "ConfigInvalid",
      );
    }
    assert.throws(
      () => new Client("http://localhost:8080", { apiKey: "" }),
      (err: unknown) => err instanceof ScopeDBError && err.kind === "ConfigInvalid",
    );
  });

  it("sends the API key as a Bearer credential and disables framework fetch caching", async () => {
    const { fn, calls } = makeFetchStub([
      jsonResponse(200, { databases: [] }, { ETag: '"v1"' }),
    ]);
    const client = new Client("http://localhost:8080", {
      fetch: fn,
      apiKey: "api-key",
    });

    await client.catalog().get();

    const init = calls[0]!.init as RequestInit;
    const headers = new Headers(init.headers);
    assert.equal(headers.get("Authorization"), "Bearer api-key");
    assert.equal(init.cache, "no-store");
  });

  it("invokes custom fetch without binding the Client instance as receiver", async () => {
    const fetch: typeof globalThis.fetch = function (
      this: unknown,
      _input: RequestInfo | URL,
      _init?: RequestInit,
    ): Promise<Response> {
      assert.equal(this, undefined);
      return Promise.resolve(
        jsonResponse(200, { databases: [] }, { ETag: '"v1"' }),
      );
    };
    const client = new Client("http://localhost:8080", { fetch });

    assert.deepEqual(await client.catalog().get(), {
      definition: { databases: [] },
      version: '"v1"',
    });
  });

  it("executes statements and returns a ResultSet via client.query()", async () => {
    const rs = {
      metadata: {
        fields: [{ name: "n", data_type: "int" as const }],
        num_rows: 1,
      },
      format: "json" as const,
      rows: [["5"]],
    };
    const { fn, calls } = makeFetchStub([
      jsonResponse(200, pendingStatus()),
      jsonResponse(200, finishedStatus(rs)),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    const result = await client.query("SELECT 5", {
      initialDelayMs: 0,
      maxDelayMs: 0,
    });

    assert.equal(calls.length, 2);
    assert.equal(result.numRows(), 1);
    assert.deepEqual(result.first(), { n: 5n });
  });
});

describe("Client error mapping", () => {
  it("maps 4xx to permanent and 429/5xx to temporary ScopeDBError", async () => {
    const { fn } = makeFetchStub([
      jsonResponse(404, { message: "not found" }),
      jsonResponse(429, { message: "rate limited" }),
      jsonResponse(503, { message: "service unavailable" }),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await assert.rejects(
      () => client.statementHandle("no-such-id").refresh(),
      (err: unknown) => err instanceof ScopeDBError && err.isPermanent() && err.message.includes("not found"),
    );
    await assert.rejects(
      () => client.catalog().get(),
      (err: unknown) => err instanceof ScopeDBError && err.isTemporary(),
    );
    await assert.rejects(
      () => client.catalog().get(),
      (err: unknown) => err instanceof ScopeDBError && err.isTemporary(),
    );
  });

  it("falls back to plain text on non-JSON error bodies and wraps transport errors", async () => {
    const { fn } = makeFetchStub([textResponse(500, "internal server error")]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await assert.rejects(
      () => client.catalog().get(),
      (err: unknown) => err instanceof ScopeDBError && err.message.includes("internal server error"),
    );
    await assert.rejects(
      () => client.catalog().get(),
      (err: unknown) => err instanceof ScopeDBError,
    );
  });

  it("preserves nested server errors and response metadata", async () => {
    const response = new Response(
      JSON.stringify({
        error: { message: "capacity is unavailable", retryable: false },
        request_id: "request-from-body",
      }),
      {
        status: 503,
        headers: {
          "Content-Type": "application/json",
          "Retry-After": "2",
          "X-Request-Id": "request-from-header",
        },
      },
    );
    const { fn } = makeFetchStub([response]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await assert.rejects(
      () => client.catalog().get(),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        assert.equal(err.message, "capacity is unavailable");
        assert.equal(err.httpStatus, 503);
        assert.equal(err.requestId, "request-from-body");
        assert.equal(err.retryable, false);
        assert.equal(err.retryAfterMs, 2_000);
        return true;
      },
    );
  });
});
