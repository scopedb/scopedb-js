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
import type {
  CatalogPage,
  DatabaseResource,
  TableResource,
} from "../src/protocol.js";
import { jsonResponse, makeFetchStub } from "./helpers.js";

const tableResource: TableResource = {
  database: "analytics",
  schema: "events",
  name: "page_views",
  columns: [
    { name: "id", data_type: "int", comment: "identifier" },
    { name: "occurred_at", data_type: "timestamp", comment: null },
  ],
  partition_by: ["id"],
  cluster_by: ["occurred_at"],
  distinct_on: { on: ["id"], by: ["occurred_at DESC"] },
  data_retention_days: 30,
  comment: "page view events",
};

describe("Client catalog API", () => {
  it("lists databases with pagination", async () => {
    const page: CatalogPage<DatabaseResource> = {
      items: [{ name: "analytics", comment: "analytics database" }],
      next_page_token: "next+/=token",
    };
    const { fn, calls } = makeFetchStub([jsonResponse(200, page)]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    const result = await client.listDatabases({
      pageSize: 25,
      pageToken: "current+/=token",
    });

    assert.deepEqual(result, page);
    assert.equal(calls.length, 1);
    const url = new URL(calls[0]!.url);
    assert.equal(url.pathname, "/v1/databases");
    assert.equal(url.searchParams.get("page_size"), "25");
    assert.equal(url.searchParams.get("page_token"), "current+/=token");
    assert.equal((calls[0]!.init as RequestInit).method, "GET");
  });

  it("fetches a database and encodes its name as one path segment", async () => {
    const database = { name: "analytics/raw", comment: null };
    const { fn, calls } = makeFetchStub([jsonResponse(200, database)]);
    const client = new Client("http://localhost:8080/api", { fetch: fn });

    assert.deepEqual(await client.fetchDatabase("analytics/raw"), database);
    assert.equal(
      calls[0]!.url,
      "http://localhost:8080/api/v1/databases/analytics%2Fraw",
    );
  });

  it("lists and fetches schemas", async () => {
    const schemas = {
      items: [{ database: "analytics", name: "events", comment: null }],
    };
    const schema = schemas.items[0]!;
    const { fn, calls } = makeFetchStub([
      jsonResponse(200, schemas),
      jsonResponse(200, schema),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    assert.deepEqual(await client.listSchemas("analytics", { pageSize: 10 }), schemas);
    assert.deepEqual(await client.fetchSchema("analytics", "events/2026"), schema);

    assert.equal(
      calls[0]!.url,
      "http://localhost:8080/v1/databases/analytics/schemas?page_size=10",
    );
    assert.equal(
      calls[1]!.url,
      "http://localhost:8080/v1/databases/analytics/schemas/events%2F2026",
    );
  });

  it("lists table summaries and fetches full table metadata", async () => {
    const tables = {
      items: [
        {
          database: "analytics",
          schema: "events",
          name: "page_views",
          comment: "page view events",
        },
      ],
    };
    const { fn, calls } = makeFetchStub([
      jsonResponse(200, tables),
      jsonResponse(200, tableResource),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    assert.deepEqual(await client.listTables("analytics", "events"), tables);
    assert.deepEqual(
      await client.fetchTable("analytics", "events", "page views?#"),
      tableResource,
    );

    assert.equal(
      calls[0]!.url,
      "http://localhost:8080/v1/databases/analytics/schemas/events/tables",
    );
    assert.equal(
      calls[1]!.url,
      "http://localhost:8080/v1/databases/analytics/schemas/events/tables/page%20views%3F%23",
    );
  });
});
