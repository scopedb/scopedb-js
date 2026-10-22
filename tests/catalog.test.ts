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
import { Client } from "../src/client.js";
import { ScopeDBError } from "../src/errors.js";
import {
  jsonResponse,
  makeFetchStub,
  parseJsonRequestBody,
} from "./helpers.js";

it("previews a replacement and applies it with the opaque ETag and escaped identity", async () => {
  const before = { name: "events/2026", tables: [] };
  const after = {
    ...before,
    tables: [
      { name: "events", columns: [{ name: "id", data_type: "int" as const }] },
    ],
  };
  const { fn, calls } = makeFetchStub([
    jsonResponse(200, before, { ETag: '\"v1\"' }),
    jsonResponse(
      200,
      { before, after, destructive: false },
      { ETag: '\"v1\"' },
    ),
    jsonResponse(
      200,
      { before, after, destructive: false },
      { ETag: '\"v2\"' },
    ),
    jsonResponse(412, {
      code: "catalog_version_mismatch",
      message: "changed",
      details: { expected: "v1" },
    }),
  ]);
  const resource = new Client("https://example.com/base", {
    apiKey: "test",
    fetch: fn,
  })
    .catalog()
    .schema("analytics", before.name);
  const snapshot = await resource.get();
  const preview = await resource.apply(after, {
    dryRun: true,
    ifMatch: snapshot.etag,
  });
  assert.deepEqual(preview.after, after);
  assert.equal(preview.etag, snapshot.etag);
  const applied = await resource.apply(after, { ifMatch: preview.etag });
  assert.equal(applied.etag, '\"v2\"');
  await assert.rejects(
    resource.apply(after, { ifMatch: preview.etag }),
    (error: unknown) => {
      assert.ok(error instanceof ScopeDBError);
      assert.equal(error.code, "catalog_version_mismatch");
      assert.equal(error.httpStatus, 412);
      assert.deepEqual(error.details, { expected: "v1" });
      assert.equal(error.retryable, false);
      return true;
    },
  );
  assert.equal(calls.length, 4);
  assert.equal(
    calls[1]!.url,
    "https://example.com/base/v1/catalog/databases/analytics/schemas/events%2F2026?dry_run=true",
  );
  assert.equal(
    new Headers(calls[2]!.init?.headers).get("If-Match"),
    snapshot.etag,
  );
  assert.deepEqual(parseJsonRequestBody(calls[2]!.init), after);
});

it("does not retry or claim rollback when a catalog write response is unreadable", async () => {
  const response = new Response("invalid json", {
    status: 200,
    headers: { ETag: '\"v2\"', "X-Request-ID": "request-1" },
  });
  const { fn, calls } = makeFetchStub([response]);
  const resource = new Client("https://example.com", { fetch: fn })
    .catalog()
    .table("scopedb", "public", "events");
  await assert.rejects(
    resource.delete({ allowDestructive: true }),
    (error: unknown) => {
      assert.ok(error instanceof ScopeDBError);
      assert.equal(error.code, "catalog_commit_outcome_unknown");
      assert.equal(error.requestId, "request-1");
      assert.equal(error.retryable, false);
      return true;
    },
  );
  assert.equal(calls.length, 1);
  assert.equal(calls[0]!.init?.method, "DELETE");
  assert.equal(
    new URL(calls[0]!.url).searchParams.get("allow_destructive"),
    "true",
  );
});

it("bounds a mutation even when a custom transport ignores cancellation", async () => {
  let requests = 0;
  const client = new Client("https://example.com", {
    fetch: async () => {
      requests++;
      return new Promise<Response>(() => {});
    },
  });
  await assert.rejects(
    client
      .catalog()
      .schema("scopedb", "events")
      .apply({ name: "events" }, { timeoutMs: 10 }),
    (error: unknown) => {
      assert.ok(error instanceof ScopeDBError);
      assert.equal(error.code, "catalog_commit_outcome_unknown");
      assert.equal(error.retryable, false);
      return true;
    },
  );
  assert.equal(requests, 1);
});

it("rejects resource names that URL normalization could turn into a parent deletion", async () => {
  const { fn, calls } = makeFetchStub([]);
  const client = new Client("https://example.com", { fetch: fn });
  for (const name of ["", ".", ".."]) {
    await assert.rejects(
      client
        .catalog()
        .table("scopedb", "public", name)
        .delete({ allowDestructive: true }),
      (error: unknown) =>
        error instanceof ScopeDBError && error.kind === "ConfigInvalid",
    );
  }
  assert.equal(calls.length, 0);
});
