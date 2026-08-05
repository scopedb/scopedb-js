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
import {
  emptyResultSet,
  finishedStatus,
  jsonResponse,
  makeFetchStub,
  pendingStatus,
  textResponse,
} from "./helpers.js";

describe("Client.submitStatement", () => {
  it("sends POST /v1/statements with correct body", async () => {
    const status = pendingStatus();
    const { fn, calls } = makeFetchStub([jsonResponse(200, status)]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await client.submitStatement({ statement: "SELECT 1", format: "json" });

    assert.equal(calls.length, 1);
    const call = calls[0]!;
    assert.ok(call.url.endsWith("/v1/statements"));
    assert.equal((call.init as RequestInit).method, "POST");
    const body = JSON.parse((call.init as RequestInit).body as string) as Record<string, unknown>;
    assert.equal(body["statement"], "SELECT 1");
    assert.equal(body["format"], "json");
  });

  it("includes optional fields when set", async () => {
    const status = pendingStatus();
    const { fn, calls } = makeFetchStub([jsonResponse(200, status)]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await client.submitStatement({
      statement: "SELECT 1",
      format: "json",
      statement_id: "my-id",
      exec_timeout: "30s",
      max_parallelism: 4,
    });

    const body = JSON.parse((calls[0]!.init as RequestInit).body as string) as Record<string, unknown>;
    assert.equal(body["statement_id"], "my-id");
    assert.equal(body["exec_timeout"], "30s");
    assert.equal(body["max_parallelism"], 4);
  });

  it("returns the statement status from the response", async () => {
    const status = pendingStatus("stmt-42");
    const { fn } = makeFetchStub([jsonResponse(200, status)]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    const result = await client.submitStatement({ statement: "SELECT 1", format: "json" });

    assert.equal(result.statement_id, "stmt-42");
    assert.equal(result.status, "pending");
  });
});

describe("Client.fetchStatement", () => {
  it("sends GET /v1/statements/{id}?format=json", async () => {
    const status = finishedStatus(emptyResultSet(), "stmt-99");
    const { fn, calls } = makeFetchStub([jsonResponse(200, status)]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await client.fetchStatement("stmt-99");

    assert.equal(calls.length, 1);
    const call = calls[0]!;
    assert.ok(call.url.includes("/v1/statements/stmt-99"), `unexpected URL: ${call.url}`);
    assert.ok(call.url.includes("format=json"), `missing format=json in URL: ${call.url}`);
    assert.equal((call.init as RequestInit).method, "GET");
  });

  it("returns the statement status from the response", async () => {
    const status = finishedStatus(emptyResultSet(), "stmt-99");
    const { fn } = makeFetchStub([jsonResponse(200, status)]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    const result = await client.fetchStatement("stmt-99");
    assert.equal(result.status, "finished");
    assert.equal(result.statement_id, "stmt-99");
  });

  it("encodes the statement id as one path segment", async () => {
    const status = finishedStatus(emptyResultSet(), "stmt/99");
    const { fn, calls } = makeFetchStub([jsonResponse(200, status)]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await client.fetchStatement("stmt/99");

    assert.equal(
      calls[0]!.url,
      "http://localhost:8080/v1/statements/stmt%2F99?format=json",
    );
  });
});

describe("Client.cancelStatement", () => {
  it("sends POST /v1/statements/{id}/cancel", async () => {
    const cancelResult = {
      statement_id: "stmt-1",
      created_at: "2024-01-01T00:00:00Z",
      status: "cancelled",
      message: "cancelled by user",
    };
    const { fn, calls } = makeFetchStub([jsonResponse(200, cancelResult)]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await client.cancelStatement("stmt-1");

    assert.equal(calls.length, 1);
    const call = calls[0]!;
    assert.ok(call.url.endsWith("/v1/statements/stmt-1/cancel"), `unexpected URL: ${call.url}`);
    assert.equal((call.init as RequestInit).method, "POST");
  });
});

describe("Client error mapping", () => {
  it("throws permanent ScopeDBError on 404 with JSON body", async () => {
    const { fn } = makeFetchStub([jsonResponse(404, { message: "not found" })]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await assert.rejects(
      () => client.fetchStatement("no-such-id"),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        assert.ok(err.message.includes("not found"), `message was: ${err.message}`);
        assert.ok(err.isPermanent(), `expected permanent, got ${err.status()}`);
        return true;
      },
    );
  });

  it("throws temporary ScopeDBError on 503", async () => {
    const { fn } = makeFetchStub([jsonResponse(503, { message: "service unavailable" })]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await assert.rejects(
      () => client.listDatabases(),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        assert.ok(err.isTemporary(), `expected temporary, got ${err.status()}`);
        return true;
      },
    );
  });

  it("throws temporary ScopeDBError on 429", async () => {
    const { fn } = makeFetchStub([jsonResponse(429, { message: "rate limited" })]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await assert.rejects(
      () => client.listDatabases(),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        assert.ok(err.isTemporary(), `expected temporary, got ${err.status()}`);
        return true;
      },
    );
  });

  it("falls back to plain-text body when response is not JSON", async () => {
    const { fn } = makeFetchStub([textResponse(500, "internal server error")]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await assert.rejects(
      () => client.listDatabases(),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        assert.ok(
          err.message.includes("internal server error"),
          `message was: ${err.message}`,
        );
        return true;
      },
    );
  });

  it("throws ScopeDBError on transport failure", async () => {
    const { fn } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await assert.rejects(
      () => client.listDatabases(),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        return true;
      },
    );
  });

  it("preserves nested server errors and response metadata", async () => {
    const response = new Response(JSON.stringify({
      error: { message: "capacity is unavailable", retryable: false },
      request_id: "request-from-body",
    }), {
      status: 503,
      headers: {
        "Content-Type": "application/json",
        "Retry-After": "2",
        "X-Request-Id": "request-from-header",
      },
    });
    const { fn } = makeFetchStub([response]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await assert.rejects(
      () => client.listDatabases(),
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

  it("disables framework fetch caching", async () => {
    const { fn, calls } = makeFetchStub([jsonResponse(200, { items: [] })]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await client.listDatabases();

    assert.equal((calls[0]!.init as RequestInit).cache, "no-store");
  });
});

describe("Client fetch transport", () => {
  it("invokes fetch without using the Client as its receiver", async () => {
    const fetch: typeof globalThis.fetch = function (
      this: unknown,
      _input: RequestInfo | URL,
      _init?: RequestInit,
    ): Promise<Response> {
      assert.equal(this, undefined);
      return Promise.resolve(jsonResponse(200, { items: [] }));
    };
    const client = new Client("http://localhost:8080", { fetch });

    assert.deepEqual(await client.listDatabases(), { items: [] });
  });
});

describe("Client.query shorthand", () => {
  it("executes statement and returns ResultSet", async () => {
    const rs = {
      metadata: { fields: [{ name: "n", data_type: "int" as const }], num_rows: 1 },
      format: "json" as const,
      rows: [["5"]],
    };
    const { fn, calls } = makeFetchStub([
      jsonResponse(200, pendingStatus()),
      jsonResponse(200, finishedStatus(rs)),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    const result = await client.query("SELECT 5", { initialDelayMs: 0, maxDelayMs: 0 });

    assert.ok(calls.length >= 1);
    assert.equal(result.numRows(), 1);
    assert.deepEqual(result.first(), { n: 5n });
  });
});

describe("ClientOptions.token", () => {
  it("sends Authorization Bearer header on every request", async () => {
    const { fn, calls } = makeFetchStub([jsonResponse(200, { items: [] })]);
    const client = new Client("http://localhost:8080", { fetch: fn, token: "my-secret" });

    await client.listDatabases();

    const headers = (calls[0]!.init as RequestInit).headers as Headers;
    assert.equal(headers.get("Authorization"), "Bearer my-secret");
  });

  it("token takes precedence over explicit Authorization header", async () => {
    const { fn, calls } = makeFetchStub([jsonResponse(200, { items: [] })]);
    const client = new Client("http://localhost:8080", {
      fetch: fn,
      token: "token-wins",
      headers: { Authorization: "Bearer other" },
    });

    await client.listDatabases();

    const headers = (calls[0]!.init as RequestInit).headers as Headers;
    assert.equal(headers.get("Authorization"), "Bearer token-wins");
  });
});

describe("ClientOptions.apiKey", () => {
  it("rejects an empty API key", () => {
    assert.throws(
      () => new Client("http://localhost:8080", { apiKey: "" }),
      (error: unknown) => {
        assert.ok(error instanceof ScopeDBError);
        assert.equal(error.kind, "ConfigInvalid");
        return true;
      },
    );
  });

  it("sends the API key as a Bearer credential", async () => {
    const { fn, calls } = makeFetchStub([jsonResponse(200, { items: [] })]);
    const client = new Client("http://localhost:8080", {
      fetch: fn,
      apiKey: "api-key",
    });

    await client.listDatabases();

    const headers = (calls[0]!.init as RequestInit).headers as Headers;
    assert.equal(headers.get("Authorization"), "Bearer api-key");
  });

  it("takes precedence over the deprecated token option", async () => {
    const { fn, calls } = makeFetchStub([jsonResponse(200, { items: [] })]);
    const client = new Client("http://localhost:8080", {
      fetch: fn,
      apiKey: "api-key-wins",
      token: "old-token",
    });

    await client.listDatabases();

    const headers = (calls[0]!.init as RequestInit).headers as Headers;
    assert.equal(headers.get("Authorization"), "Bearer api-key-wins");
  });
});
