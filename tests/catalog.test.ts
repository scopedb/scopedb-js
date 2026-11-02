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
import { jsonResponse, makeFetchStub, requestBodyText } from "./helpers.js";

describe("Catalog and Table.catalog()", () => {
  it("previews a replacement and applies it with the opaque version and escaped identity", async () => {
    const before = { name: "events/2026", tables: [] };
    const after = {
      ...before,
      tables: [
        { name: "events", columns: [{ name: "id", data_type: "int" as const }] },
      ],
    };
    const { fn, calls } = makeFetchStub([
      jsonResponse(200, before, { ETag: '"v1"' }),
      jsonResponse(
        200,
        { before, after, destructive: false },
        { ETag: '"v1"' },
      ),
      jsonResponse(
        200,
        { before, after, destructive: false },
        { ETag: '"v2"' },
      ),
      jsonResponse(412, {
        code: "precondition_failed",
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
      expectedVersion: snapshot.version,
    });
    assert.deepEqual(preview.after, after);
    assert.equal(preview.version, snapshot.version);
    const applied = await resource.apply(after, {
      expectedVersion: preview.version,
    });
    assert.equal(applied.version, '"v2"');
    await assert.rejects(
      resource.apply(after, { expectedVersion: preview.version }),
      (error: unknown) => {
        assert.ok(error instanceof ScopeDBError);
        assert.equal(error.code, "precondition_failed");
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
      snapshot.version,
    );
    assert.deepEqual(await new Response(calls[2]!.init?.body).json(), after);
  });

  it("describes a table via Table.catalog() and round-trips nested extension fields", async () => {
    const definition = {
      name: "events",
      future_table_option: { mode: "new" },
      columns: [
        {
          name: "id",
          data_type: "int" as const,
          future_column_option: [1, 2],
        },
      ],
    };
    const { fn, calls } = makeFetchStub([
      jsonResponse(200, definition, { ETag: '"original"' }),
      jsonResponse(
        200,
        {
          before: definition,
          after: { ...definition, comment: "updated" },
          destructive: false,
        },
        { ETag: '"next"' },
      ),
    ]);
    const client = new Client("https://example.com", { fetch: fn });
    const resource = client
      .table("events", { database: "analytics", schema: "logs" })
      .catalog();
    const snapshot = await resource.get();
    assert.equal(
      calls[0]!.url,
      "https://example.com/v1/catalog/databases/analytics/schemas/logs/tables/events",
    );
    snapshot.definition.comment = "updated";
    const applied = await resource.apply(snapshot.definition, {
      expectedVersion: snapshot.version,
    });
    assert.equal(new Headers(calls[1]!.init?.headers).get("if-match"), '"original"');
    assert.deepEqual(JSON.parse(requestBodyText(calls[1]!.init)), {
      ...definition,
      comment: "updated",
    });
    assert.deepEqual(applied.after?.columns?.[0]?.["future_column_option"], [1, 2]);
  });

  it("rejects unsafe resource names and invalid null fields locally or on read", async () => {
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("https://example.com", { fetch: fn });
    for (const name of ["", ".", ".."]) {
      await assert.rejects(
        client
          .catalog()
          .table("scopedb", "public", name)
          .delete({ allowDestructive: true }),
        (err: unknown) => err instanceof ScopeDBError && err.kind === "ConfigInvalid",
      );
    }
    assert.equal(calls.length, 0);

    for (const badDef of [
      { name: "events", columns: "invalid" },
      { name: "events", storage: null },
      {
        name: "events",
        columns: [
          {
            name: "message",
            data_type: "string",
            indexes: [{ type: "search", analyzer: null }],
          },
        ],
      },
    ]) {
      const badClient = new Client("https://example.com", {
        fetch: async () => jsonResponse(200, badDef, { ETag: '"v1"' }),
      });
      await assert.rejects(badClient.table("events").catalog().get(), ScopeDBError);
    }
  });

  it("classifies catalogOutcome across unreadable, gateway, coded, and timed-out mutations", async (t) => {
    // Unreadable 200 response on delete -> unknown outcome
    const unreadable = new Response("invalid json", {
      status: 200,
      headers: { ETag: '"v2"', "X-Request-ID": "request-1" },
    });
    const { fn, calls } = makeFetchStub([unreadable]);
    const resource = new Client("https://example.com", { fetch: fn })
      .catalog()
      .table("scopedb", "public", "events");
    await assert.rejects(
      resource.delete({ allowDestructive: true }),
      (error: unknown) => {
        assert.ok(error instanceof ScopeDBError);
        assert.equal(error.catalogOutcome, "unknown");
        assert.equal(error.requestId, "request-1");
        assert.equal(error.retryable, false);
        return true;
      },
    );
    assert.equal(calls[0]!.init?.method, "DELETE");
    assert.equal(
      new URL(calls[0]!.url).searchParams.get("allow_destructive"),
      "true",
    );

    // Malformed 200 response on apply -> unknown outcome
    const malformedClient = new Client("https://example.com", {
      fetch: async () =>
        jsonResponse(
          200,
          { before: null, after: { name: "events", columns: "invalid" }, destructive: false },
          { ETag: '"v1"' },
        ),
    });
    await assert.rejects(
      malformedClient.table("events").catalog().apply({ name: "events" }),
      (err: unknown) => err instanceof ScopeDBError && err.catalogOutcome === "unknown",
    );

    // Unstructured 408 / 429 / 502 responses on write -> unknown outcome, not retryable
    for (const status of [408, 429, 502]) {
      const gwClient = new Client("https://example.com", {
        fetch: async () => jsonResponse(status, { message: "gateway error" }),
      });
      await assert.rejects(
        gwClient.table("events").catalog().apply({ name: "events" }),
        (err: unknown) =>
          err instanceof ScopeDBError &&
          err.catalogOutcome === "unknown" &&
          !err.retryable,
      );
    }

    // Coded catalog failures across write, read, and dry-run preview
    for (const [status, code, outcome] of [
      [412, "precondition_failed", "rejected"],
      [408, "request_timeout", "unknown"],
      [503, "service_unavailable", "unknown"],
    ] as const) {
      for (const mode of ["write", "read", "preview"] as const) {
        const codedResource = new Client("https://example.com", {
          fetch: async () =>
            Response.json(
              {
                error: { code, message: "failed", details: { reason: "test" } },
                request_id: "catalog-request",
              },
              { status },
            ),
        })
          .table("events")
          .catalog();
        await assert.rejects(
          mode === "read"
            ? codedResource.get()
            : codedResource.apply({ name: "events" }, { dryRun: mode === "preview" }),
          (error: unknown) => {
            assert.ok(error instanceof ScopeDBError);
            assert.equal(
              error.catalogOutcome,
              mode === "write" ? outcome : undefined,
            );
            assert.equal(error.code, code);
            assert.equal(error.requestId, "catalog-request");
            assert.deepEqual(error.details, { reason: "test" });
            if (mode === "write") assert.equal(error.retryable, false);
            return true;
          },
        );
      }
    }

    // Per-call timeoutMs and disabling mutation deadline with timeoutMs: null
    let stalledRequests = 0;
    const stalledClient = new Client("https://example.com", {
      fetch: async () => {
        stalledRequests++;
        return new Promise<Response>(() => {});
      },
    });
    await assert.rejects(
      stalledClient
        .catalog()
        .schema("scopedb", "events")
        .apply({ name: "events" }, { timeoutMs: 10 }),
      (error: unknown) => {
        assert.ok(error instanceof ScopeDBError);
        assert.equal(error.catalogOutcome, "unknown");
        assert.deepEqual(error.writeDetails, {
          reason: "timeout",
          outcome: "ambiguous",
        });
        assert.equal(error.retryable, false);
        return true;
      },
    );
    assert.equal(stalledRequests, 1);

    t.mock.timers.enable({ apis: ["setTimeout"] });
    let capturedSignal: AbortSignal | undefined;
    let ready!: () => void;
    const readyPromise = new Promise<void>((r) => {
      ready = r;
    });
    const noDeadlineClient = new Client("https://example.com", {
      writeTimeoutMs: 1,
      fetch: async (_url, init) => {
        capturedSignal = init?.signal ?? undefined;
        ready();
        return new Promise<Response>(() => {});
      },
    });
    const controller = new AbortController();
    const rejected = assert.rejects(
      noDeadlineClient.table("events").catalog().apply(
        { name: "events" },
        { timeoutMs: null, signal: controller.signal },
      ),
      (err: unknown) =>
        err instanceof ScopeDBError &&
        err.catalogOutcome === "unknown" &&
        err.writeDetails?.reason === "cancelled",
    );
    await readyPromise;
    t.mock.timers.tick(60_000);
    assert.equal(capturedSignal?.aborted, false);
    controller.abort();
    await rejected;
  });
});
