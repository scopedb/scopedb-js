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
import { AppendRowsError } from "../src/errors.js";
import { Table } from "../src/table.js";
import {
  emptyResultSet,
  finishedStatus,
  jsonResponse,
  makeFetchStub,
  parseJsonRequestBody,
  requestBodyText,
} from "./helpers.js";

describe("Table.identifier — ScopeQL quoting", () => {
  function makeTable(name: string): Table {
    // fetch stub is irrelevant for identifier(); just needs a client
    const { fn } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    return new Table(client, name);
  }

  it("wraps simple table name in backticks", () => {
    assert.equal(makeTable("events").identifier(), "`events`");
  });

  it("escapes backtick in table name", () => {
    assert.equal(makeTable("my`table").identifier(), "`my\\`table`");
  });

  it("includes database and schema when set", () => {
    const { fn } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const table = new Table(client, "events").withDatabase("mydb").withSchema("myschema");
    assert.equal(table.identifier(), "`mydb`.`myschema`.`events`");
  });

  it("includes only schema when database is not set", () => {
    const { fn } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const table = new Table(client, "events").withSchema("logs");
    assert.equal(table.identifier(), "`logs`.`events`");
  });

  it("includes only database when schema is not set", () => {
    const { fn } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const table = new Table(client, "events").withDatabase("mydb");
    assert.equal(table.identifier(), "`mydb`.`events`");
  });

  it("escapes tab, newline, carriage return in table name", () => {
    assert.equal(makeTable("tab\there").identifier(), "`tab\\there`");
    assert.equal(makeTable("new\nline").identifier(), "`new\\nline`");
    assert.equal(makeTable("cr\rhere").identifier(), "`cr\\rhere`");
  });

  it("escapes backslash in table name", () => {
    assert.equal(makeTable("back\\slash").identifier(), "`back\\\\slash`");
  });

  it("escapes control characters using hex notation", () => {
    // 0x01 should become \x01
    const name = "ctrl\x01char";
    const id = makeTable(name).identifier();
    assert.ok(id.includes("\\x01"), `expected \\x01 in: ${id}`);
  });
});

describe("Table.drop", () => {
  it("executes DROP TABLE with the correct identifier", async () => {
    // drop() calls statement(...).execute(), which does POST /v1/statements then GET
    const finished = finishedStatus(emptyResultSet());
    const { fn, calls } = makeFetchStub([
      jsonResponse(200, { status: "pending", statement_id: "s1", created_at: "2024-01-01T00:00:00Z", progress: { total_percentage: 0, nanos_from_submitted: 0, nanos_from_started: 0, total_stages: 0, total_partitions: 0, total_rows: 0, total_compressed_bytes: 0, total_uncompressed_bytes: 0, scanned_stages: 0, scanned_partitions: 0, scanned_rows: 0, scanned_compressed_bytes: 0, scanned_uncompressed_bytes: 0, skipped_partitions: 0, skipped_rows: 0, skipped_compressed_bytes: 0, skipped_uncompressed_bytes: 0 } }),
      jsonResponse(200, finished),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const table = new Table(client, "my_table");

    await table.drop({ initialDelayMs: 0, maxDelayMs: 0 } as Parameters<typeof table.drop>[0]);

    // First call is POST /v1/statements; verify the statement contains the quoted identifier
    const body = parseJsonRequestBody(calls[0]!.init) as Record<string, unknown>;
    assert.ok(
      (body["statement"] as string).includes("`my_table`"),
      `statement was: ${body["statement"]}`,
    );
    assert.ok(
      (body["statement"] as string).toLowerCase().includes("drop table"),
      `statement was: ${body["statement"]}`,
    );
  });
});

describe("Table.append", () => {
  it("posts the NDJSON payload to the fully qualified rows endpoint", async () => {
    const appendResult = { append_state: "committed", num_rows_inserted: 2 };
    const { fn, calls } = makeFetchStub([jsonResponse(200, appendResult)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const table = client
      .table("page views?#")
      .withDatabase("analytics/raw")
      .withSchema("events 2026");
    const ndjson = '{"id":1}\n{"id":2}\n';

    assert.deepEqual(await table.append(ndjson), appendResult);

    assert.equal(calls.length, 1);
    const call = calls[0]!;
    assert.equal(
      call.url,
      "http://localhost:8080/v1/databases/analytics%2Fraw/schemas/events%202026/tables/page%20views%3F%23/rows",
    );
    const init = call.init as RequestInit;
    assert.equal(init.method, "POST");
    assert.equal(requestBodyText(init), ndjson);
    const headers = init.headers as Headers;
    assert.equal(headers.get("Content-Type"), "application/x-ndjson");
    assert.equal(headers.get("Accept"), "application/json");
    assert.equal(headers.get("Content-Encoding"), "gzip");
  });

  it("uses the default database and schema", async () => {
    const { fn, calls } = makeFetchStub([
      jsonResponse(200, { append_state: "committed", num_rows_inserted: 1 }),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await client.table("events").append('{"id":1}');

    assert.equal(
      calls[0]!.url,
      "http://localhost:8080/v1/databases/scopedb/schemas/public/tables/events/rows",
    );
  });

  it("accepts the table location when the handle is created", async () => {
    const { fn, calls } = makeFetchStub([
      jsonResponse(200, { append_state: "committed", num_rows_inserted: 1 }),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await client.table("events", {
      database: "analytics",
      schema: "logs",
    }).append('{"id":1}');

    assert.equal(
      calls[0]!.url,
      "http://localhost:8080/v1/databases/analytics/schemas/logs/tables/events/rows",
    );
  });

  it("rejects an uncompressed NDJSON body over 8 MiB before sending", async () => {
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const oversized = "x".repeat(8 * 1024 * 1024 + 1);

    await assert.rejects(
      () => client.table("events").append(oversized),
      (error: unknown) => {
        assert.ok(error instanceof AppendRowsError);
        assert.equal(error.appendState, "rejected");
        assert.match(error.message, /8388608-byte append limit/);
        return true;
      },
    );
    assert.equal(calls.length, 0);
  });

  it("does not start an append when the signal is already aborted", async () => {
    const { fn, calls } = makeFetchStub([
      jsonResponse(200, { append_state: "committed", num_rows_inserted: 1 }),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const controller = new AbortController();
    const reason = new DOMException("cancelled", "AbortError");
    controller.abort(reason);

    await assert.rejects(
      () => client.table("events").append('{"id":1}', {
        signal: controller.signal,
      }),
      (error: unknown) => error === reason,
    );
    assert.equal(calls.length, 0);
  });

  it("preserves row-level rejection details", async () => {
    const { fn } = makeFetchStub([
      jsonResponse(422, {
        message: "row validation failed",
        append_state: "rejected",
        row_errors: [
          { row_index: 1, column: "id", message: "expected int, got string" },
        ],
        row_errors_truncated: false,
      }),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await assert.rejects(
      () => client.table("events").append('{"id":"bad"}'),
      (error: unknown) => {
        assert.ok(error instanceof AppendRowsError);
        assert.equal(error.appendState, "rejected");
        assert.deepEqual(error.rowErrors, [
          { row_index: 1, column: "id", message: "expected int, got string" },
        ]);
        assert.equal(error.rowErrorsTruncated, false);
        assert.ok(error.isPermanent());
        return true;
      },
    );
  });

  it("does not mark an unknown commit outcome as automatically retryable", async () => {
    const { fn } = makeFetchStub([
      jsonResponse(503, {
        message: "append commit outcome is unknown",
        append_state: "unknown",
        row_errors: [],
        row_errors_truncated: false,
      }),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await assert.rejects(
      () => client.table("events").append('{"id":1}'),
      (error: unknown) => {
        assert.ok(error instanceof AppendRowsError);
        assert.equal(error.appendState, "unknown");
        assert.ok(error.isPersistent());
        return true;
      },
    );
  });

  it("preserves response metadata when a successful append body is malformed", async () => {
    const responses = [
      new Response("not-json", {
        status: 200,
        headers: { "X-Request-Id": "req-malformed-json" },
      }),
      new Response("{}", {
        status: 200,
        headers: {
          "Content-Type": "application/json",
          "X-Request-Id": "req-invalid-shape",
        },
      }),
    ];
    const { fn } = makeFetchStub(responses);
    const client = new Client("http://localhost:8080", { fetch: fn });

    for (const requestId of ["req-malformed-json", "req-invalid-shape"]) {
      await assert.rejects(
        () => client.table("events").append('{"id":1}'),
        (error: unknown) => {
          assert.ok(error instanceof AppendRowsError);
          assert.equal(error.appendState, "unknown");
          assert.equal(error.httpStatus, 200);
          assert.equal(error.requestId, requestId);
          return true;
        },
      );
    }
  });
});

describe("Table.tableSchema", () => {
  it("uses the RESTful table resource", async () => {
    const resource = {
      database: "analytics",
      schema: "events",
      name: "page_views",
      columns: [
        { name: "id", data_type: "int", comment: "identifier" },
        { name: "occurred_at", data_type: "timestamp", comment: null },
      ],
      partition_by: [],
      cluster_by: [],
      distinct_on: { on: [], by: [] },
      data_retention_days: null,
      comment: null,
    };
    const { fn, calls } = makeFetchStub([jsonResponse(200, resource)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const table = client
      .table("page_views")
      .withDatabase("analytics")
      .withSchema("events");

    const schema = await table.tableSchema();

    assert.deepEqual(
      schema.fields().map((field) => [field.name(), field.dataType()]),
      [
        ["id", "int"],
        ["occurred_at", "timestamp"],
      ],
    );
    assert.equal(calls.length, 1);
    assert.equal((calls[0]!.init as RequestInit).method, "GET");
    assert.equal(
      calls[0]!.url,
      "http://localhost:8080/v1/databases/analytics/schemas/events/tables/page_views",
    );
  });

  it("describes the complete table with JS-style field names", async () => {
    const resource = {
      database: "analytics",
      schema: "events",
      name: "page_views",
      columns: [
        { name: "id", data_type: "int", comment: "identifier" },
      ],
      partition_by: ["day"],
      cluster_by: ["id"],
      distinct_on: { on: ["id"], by: ["occurred_at DESC"] },
      data_retention_days: 30,
      comment: "page views",
    };
    const { fn } = makeFetchStub([jsonResponse(200, resource)]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    const description = await client.table("page_views", {
      database: "analytics",
      schema: "events",
    }).describe();

    assert.deepEqual(description, {
      database: "analytics",
      schema: "events",
      name: "page_views",
      columns: [{ name: "id", dataType: "int", comment: "identifier" }],
      partitionBy: ["day"],
      clusterBy: ["id"],
      distinctOn: { on: ["id"], by: ["occurred_at DESC"] },
      dataRetentionDays: 30,
      comment: "page views",
    });
  });

  it("does not let request options override the table handle identity", async () => {
    const resource = {
      database: "analytics",
      schema: "events",
      name: "page_views",
      columns: [],
      partition_by: [],
      cluster_by: [],
      distinct_on: { on: [], by: [] },
      data_retention_days: null,
      comment: null,
    };
    const { fn, calls } = makeFetchStub([jsonResponse(200, resource)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const widerOptions = {
      database: "other",
      schema: "other",
      table: "other",
    } as unknown as { signal?: AbortSignal };

    await client.table("page_views", {
      database: "analytics",
      schema: "events",
    }).describe(widerOptions);

    assert.equal(
      calls[0]!.url,
      "http://localhost:8080/v1/databases/analytics/schemas/events/tables/page_views",
    );
  });
});
