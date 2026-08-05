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

import { AppendRowsError, ScopeDBError, asScopeDBError } from "./errors.js";
import { IngestStreamBuilder } from "./ingest-stream.js";
import type {
  AppendRowError,
  AppendRowsErrorPayload,
  AppendRowsResult,
  DatabaseResource,
  ErrorPayload,
  IngestRequest,
  IngestResult,
  ResourceCollection,
  SchemaResource,
  StatementCancelResult,
  StatementRequest,
  StatementStatus,
  TableResource,
  TableResourceSummary,
} from "./protocol.js";
import type { ResultSet } from "./result.js";
import { Statement, StatementHandle } from "./statement.js";
import type { FetchOptions } from "./statement.js";
import { Table } from "./table.js";

export interface RequestOptions {
  signal?: AbortSignal;
}

export interface CatalogListOptions extends RequestOptions {
  /** Number of resources to return. The server accepts values from 1 to 1000. */
  pageSize?: number;
  /** Opaque token returned as `next_page_token` by the previous page. */
  pageToken?: string;
}

export interface ClientOptions {
  fetch?: typeof globalThis.fetch;
  /** Default headers sent with every request. */
  headers?: HeadersInit;
  /**
   * Bearer token for authentication.
   * Equivalent to `headers: { Authorization: 'Bearer <token>' }`.
   * If both `token` and `headers.Authorization` are provided, `token` wins.
   */
  token?: string;
}

export class Client {
  private readonly endpoint: URL;
  private readonly fetchFn: typeof globalThis.fetch;
  private readonly defaultHeaders: Headers;

  constructor(endpoint: string | URL, options: ClientOptions = {}) {
    try {
      this.endpoint = normalizeEndpoint(endpoint);
    } catch (cause) {
      throw new ScopeDBError("ConfigInvalid", "failed to parse endpoint", { cause });
    }

    this.fetchFn = options.fetch ?? globalThis.fetch;
    this.defaultHeaders = new Headers(options.headers);
    if (options.token !== undefined) {
      this.defaultHeaders.set("Authorization", `Bearer ${options.token}`);
    }
  }

  statement(statement: string): Statement {
    return new Statement(this, statement);
  }

  statementHandle(statementId: string): StatementHandle {
    return new StatementHandle(this, statementId);
  }

  table(table: string): Table {
    return new Table(this, table);
  }

  ingestStream(statement: string): IngestStreamBuilder {
    return new IngestStreamBuilder(this, statement);
  }

  /**
   * Executes a ScopeQL statement and returns all rows.
   *
   * Shorthand for `client.statement(sql).execute(options)`.
   *
   * @example
   * const result = await client.query("SELECT * FROM events LIMIT 10");
   * for (const row of result.intoObjects()) {
   *   console.log(row);
   * }
   */
  async query(sql: string, options: FetchOptions = {}): Promise<ResultSet> {
    return this.statement(sql).execute(options);
  }

  async healthCheck(options: RequestOptions = {}): Promise<void> {
    await this.request("v1/health", {
      method: "GET",
      signal: options.signal,
    });
  }

  async listDatabases(
    options: CatalogListOptions = {},
  ): Promise<ResourceCollection<DatabaseResource>> {
    return this.requestJson(this.catalogUrl(["databases"], options), {
      method: "GET",
      signal: options.signal,
    });
  }

  async fetchDatabase(
    database: string,
    options: RequestOptions = {},
  ): Promise<DatabaseResource> {
    return this.requestJson(this.resourceUrl(["databases", database]), {
      method: "GET",
      signal: options.signal,
    });
  }

  async listSchemas(
    database: string,
    options: CatalogListOptions = {},
  ): Promise<ResourceCollection<SchemaResource>> {
    return this.requestJson(
      this.catalogUrl(["databases", database, "schemas"], options),
      {
        method: "GET",
        signal: options.signal,
      },
    );
  }

  async fetchSchema(
    database: string,
    schema: string,
    options: RequestOptions = {},
  ): Promise<SchemaResource> {
    return this.requestJson(
      this.resourceUrl(["databases", database, "schemas", schema]),
      {
        method: "GET",
        signal: options.signal,
      },
    );
  }

  async listTables(
    database: string,
    schema: string,
    options: CatalogListOptions = {},
  ): Promise<ResourceCollection<TableResourceSummary>> {
    return this.requestJson(
      this.catalogUrl(
        ["databases", database, "schemas", schema, "tables"],
        options,
      ),
      {
        method: "GET",
        signal: options.signal,
      },
    );
  }

  async fetchTable(
    database: string,
    schema: string,
    table: string,
    options: RequestOptions = {},
  ): Promise<TableResource> {
    return this.requestJson(
      this.resourceUrl([
        "databases",
        database,
        "schemas",
        schema,
        "tables",
        table,
      ]),
      {
        method: "GET",
        signal: options.signal,
      },
    );
  }

  async appendRows(
    database: string,
    schema: string,
    table: string,
    ndjson: string,
    options: RequestOptions = {},
  ): Promise<AppendRowsResult> {
    const url = this.resourceUrl([
      "databases",
      database,
      "schemas",
      schema,
      "tables",
      table,
      "rows",
    ]);
    try {
      const result: unknown = await this.requestJson(url, {
        method: "POST",
        headers: { "Content-Type": "application/x-ndjson" },
        body: ndjson,
        signal: options.signal,
      });
      if (!isAppendRowsResult(result)) {
        throw new ScopeDBError(
          "Unexpected",
          "append response has an invalid body",
        );
      }
      return result;
    } catch (cause) {
      if (cause instanceof AppendRowsError) {
        throw cause;
      }
      const error = asScopeDBError(
        "Unexpected",
        "failed to append rows",
        cause,
      );

      // Once the request leaves the client, a transport or intermediary failure
      // (including an invalid success response) cannot prove whether the append
      // committed. Retrying could duplicate rows.
      throw new AppendRowsError(
        {
          message: error.message,
          append_state: "unknown",
          row_errors: [],
          row_errors_truncated: false,
        },
        error.message,
        { cause: error },
      ).setPersistent();
    }
  }

  async insert(
    rows: string,
    transform: string,
    options: RequestOptions = {},
  ): Promise<IngestResult> {
    return this.ingest(
      {
        type: "committed",
        data: { format: "json", rows },
        statement: transform,
      },
      options,
    );
  }

  async submitStatement(
    request: StatementRequest,
    options: RequestOptions = {},
  ): Promise<StatementStatus> {
    return this.requestJson("v1/statements", {
      method: "POST",
      body: JSON.stringify(request),
      signal: options.signal,
    });
  }

  async fetchStatement(
    statementId: string,
    options: RequestOptions = {},
  ): Promise<StatementStatus> {
    const url = this.makeUrl(`v1/statements/${statementId}`);
    url.searchParams.set("format", "json");
    return this.requestJson(url, {
      method: "GET",
      signal: options.signal,
    });
  }

  async cancelStatement(
    statementId: string,
    options: RequestOptions = {},
  ): Promise<StatementCancelResult> {
    return this.requestJson(`v1/statements/${statementId}/cancel`, {
      method: "POST",
      signal: options.signal,
    });
  }

  async ingest(
    request: IngestRequest,
    options: RequestOptions = {},
  ): Promise<IngestResult> {
    return this.requestJson("v1/ingest", {
      method: "POST",
      body: JSON.stringify(request),
      signal: options.signal,
    });
  }

  private async requestJson<T>(
    path: string | URL,
    init: RequestInit,
  ): Promise<T> {
    const response = await this.request(path, init);
    try {
      return (await response.json()) as T;
    } catch (cause) {
      throw new ScopeDBError("Unexpected", "failed to parse response body", {
        cause,
      });
    }
  }

  private async request(path: string | URL, init: RequestInit): Promise<Response> {
    const headers = new Headers(this.defaultHeaders);
    headers.set("Accept", "application/json");
    if (init.body !== undefined) {
      headers.set("Content-Type", "application/json");
    }
    const providedHeaders = new Headers(init.headers);
    providedHeaders.forEach((value, key) => headers.set(key, value));

    const url = path instanceof URL ? path : this.makeUrl(path);

    let response: Response;
    try {
      response = await this.fetchFn(url, { ...init, headers });
    } catch (cause) {
      throw asScopeDBError("Unexpected", `failed to send request to ${url}`, cause).setTemporary();
    }

    if (response.ok) {
      return response;
    }

    throw await responseToError(response);
  }

  private makeUrl(path: string): URL {
    return new URL(path, this.endpoint);
  }

  private resourceUrl(segments: readonly string[]): URL {
    return this.makeUrl(["v1", ...segments].map(encodeURIComponent).join("/"));
  }

  private catalogUrl(
    segments: readonly string[],
    options: CatalogListOptions,
  ): URL {
    const url = this.resourceUrl(segments);
    if (options.pageSize !== undefined) {
      url.searchParams.set("page_size", String(options.pageSize));
    }
    if (options.pageToken !== undefined) {
      url.searchParams.set("page_token", options.pageToken);
    }
    return url;
  }
}

function normalizeEndpoint(endpoint: string | URL): URL {
  const url = new URL(endpoint.toString());
  if (!url.pathname.endsWith("/")) {
    url.pathname = `${url.pathname}/`;
  }
  return url;
}

async function responseToError(response: Response): Promise<ScopeDBError> {
  const body = await response.text();
  let message = body;
  let appendPayload: AppendRowsErrorPayload | undefined;
  try {
    const payload: unknown = JSON.parse(body);
    if (isErrorPayload(payload)) {
      if (payload.message.length > 0) {
        message = payload.message;
      }
      appendPayload = asAppendRowsErrorPayload(payload);
    }
  } catch {
    // Fall back to the raw response body.
  }

  const errorMessage = `${response.status} ${response.statusText}: ${message}`;
  const error = appendPayload === undefined
    ? new ScopeDBError("Unexpected", errorMessage)
    : new AppendRowsError(appendPayload, errorMessage);
  if (error instanceof AppendRowsError && error.appendState === "unknown") {
    // Retrying an append with an unknown commit outcome can insert duplicates.
    return error.setPersistent();
  }
  if (response.status === 408 || response.status === 429 || response.status >= 500) {
    return error.setTemporary();
  }
  return error.setPermanent();
}

function isErrorPayload(value: unknown): value is ErrorPayload & Record<string, unknown> {
  return isRecord(value) && typeof value["message"] === "string";
}

function asAppendRowsErrorPayload(
  value: ErrorPayload & Record<string, unknown>,
): AppendRowsErrorPayload | undefined {
  const appendState = value["append_state"];
  if (appendState !== "rejected" && appendState !== "unknown") {
    return undefined;
  }

  const rowErrors = value["row_errors"];
  if (rowErrors !== undefined && !isAppendRowErrors(rowErrors)) {
    return undefined;
  }
  const rowErrorsTruncated = value["row_errors_truncated"];
  if (rowErrorsTruncated !== undefined && typeof rowErrorsTruncated !== "boolean") {
    return undefined;
  }

  return {
    message: value.message,
    append_state: appendState,
    row_errors: rowErrors ?? [],
    row_errors_truncated: rowErrorsTruncated ?? false,
  };
}

function isAppendRowErrors(value: unknown): value is AppendRowError[] {
  return Array.isArray(value) && value.every((row) =>
    isRecord(row) &&
    typeof row["row_index"] === "number" &&
    typeof row["column"] === "string" &&
    typeof row["message"] === "string"
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isAppendRowsResult(value: unknown): value is AppendRowsResult {
  return isRecord(value) &&
    value["append_state"] === "committed" &&
    typeof value["num_rows_inserted"] === "number" &&
    Number.isSafeInteger(value["num_rows_inserted"]) &&
    value["num_rows_inserted"] >= 0;
}
