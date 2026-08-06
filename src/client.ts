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
  CatalogPage,
  DatabaseResource,
  IngestRequest,
  IngestResult,
  SchemaResource,
  StatementCancelResult,
  StatementRequest,
  StatementStatus,
  TableResource,
  TableResourceSummary,
} from "./protocol.js";
import type { ResultSet } from "./result.js";
import { Statement, StatementHandle } from "./statement.js";
import type { WaitOptions } from "./statement.js";
import { Table, type TableOptions } from "./table.js";

export interface RequestOptions {
  signal?: AbortSignal;
}

export interface CatalogListOptions extends RequestOptions {
  /** Number of resources to return. The server accepts values from 1 to 1000. */
  pageSize?: number;
  /** Opaque token returned as `next_page_token` by the previous page. */
  pageToken?: string;
}

export interface SchemaCatalogListOptions extends CatalogListOptions {
  database: string;
}

export interface TableCatalogListOptions extends CatalogListOptions {
  database: string;
  schema: string;
}

export interface SchemaReference extends RequestOptions {
  database: string;
  schema: string;
}

export interface TableReference extends SchemaReference {
  table: string;
}

export interface ClientOptions {
  fetch?: typeof globalThis.fetch;
  /** Default headers sent with every request. */
  headers?: HeadersInit;
  /**
   * ScopeDB API key. It is sent as a Bearer credential and must only be used
   * from trusted server-side code. It takes precedence over `token` and an
   * `Authorization` value in `headers`.
   */
  apiKey?: string;
  /**
   * Bearer token for authentication.
   * Equivalent to `headers: { Authorization: 'Bearer <token>' }`.
   * If `apiKey` is also provided, `apiKey` wins.
   *
   * @deprecated Use `apiKey`.
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

    const fetchFn = options.fetch ?? globalThis.fetch;
    // Some Web runtimes require fetch to be called without an arbitrary
    // receiver. Calling a stored function as `this.fetchFn()` would otherwise
    // pass the Client instance as `this` and fail with an illegal invocation.
    this.fetchFn = (input, init) => fetchFn(input, init);
    this.defaultHeaders = new Headers(options.headers);
    const apiKey = options.apiKey ?? options.token;
    if (apiKey !== undefined) {
      if (apiKey.length === 0) {
        throw new ScopeDBError("ConfigInvalid", "apiKey must not be empty");
      }
      this.defaultHeaders.set("Authorization", `Bearer ${apiKey}`);
    }
  }

  statement(scopeql: string): Statement {
    return new Statement(this, scopeql);
  }

  statementHandle(statementId: string): StatementHandle {
    return new StatementHandle(this, statementId);
  }

  table(table: string, options: TableOptions = {}): Table {
    return new Table(this, table, options);
  }

  ingestStream(statement: string): IngestStreamBuilder {
    return new IngestStreamBuilder(this, statement);
  }

  /**
   * Executes a ScopeQL statement and returns all rows.
   *
   * Shorthand for `client.statement(scopeql).execute(options)`.
   *
   * @example
   * const result = await client.query("FROM events SELECT * LIMIT 10");
   * for (const row of result.toObjects()) {
   *   console.log(row);
   * }
   */
  async query(scopeql: string, options: WaitOptions = {}): Promise<ResultSet> {
    return this.statement(scopeql).execute(options);
  }

  async listDatabases(
    options: CatalogListOptions = {},
  ): Promise<CatalogPage<DatabaseResource>> {
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
    reference: SchemaCatalogListOptions,
  ): Promise<CatalogPage<SchemaResource>>;
  async listSchemas(
    database: string,
    options?: CatalogListOptions,
  ): Promise<CatalogPage<SchemaResource>>;
  async listSchemas(
    databaseOrReference: string | SchemaCatalogListOptions,
    options: CatalogListOptions = {},
  ): Promise<CatalogPage<SchemaResource>> {
    const [database, listOptions] = schemaListArguments(
      databaseOrReference,
      options,
    );
    return this.requestJson(
      this.catalogUrl(["databases", database, "schemas"], listOptions),
      {
        method: "GET",
        signal: listOptions.signal,
      },
    );
  }

  async fetchSchema(reference: SchemaReference): Promise<SchemaResource>;
  async fetchSchema(
    database: string,
    schema: string,
    options?: RequestOptions,
  ): Promise<SchemaResource>;
  async fetchSchema(
    databaseOrReference: string | SchemaReference,
    schema?: string,
    options: RequestOptions = {},
  ): Promise<SchemaResource> {
    const [database, schemaName, requestOptions] = schemaReferenceArguments(
      databaseOrReference,
      schema,
      options,
    );
    return this.requestJson(
      this.resourceUrl(["databases", database, "schemas", schemaName]),
      {
        method: "GET",
        signal: requestOptions.signal,
      },
    );
  }

  async listTables(
    reference: TableCatalogListOptions,
  ): Promise<CatalogPage<TableResourceSummary>>;
  async listTables(
    database: string,
    schema: string,
    options?: CatalogListOptions,
  ): Promise<CatalogPage<TableResourceSummary>>;
  async listTables(
    databaseOrReference: string | TableCatalogListOptions,
    schema?: string,
    options: CatalogListOptions = {},
  ): Promise<CatalogPage<TableResourceSummary>> {
    const [database, schemaName, listOptions] = tableListArguments(
      databaseOrReference,
      schema,
      options,
    );
    return this.requestJson(
      this.catalogUrl(
        ["databases", database, "schemas", schemaName, "tables"],
        listOptions,
      ),
      {
        method: "GET",
        signal: listOptions.signal,
      },
    );
  }

  async fetchTable(reference: TableReference): Promise<TableResource>;
  async fetchTable(
    database: string,
    schema: string,
    table: string,
    options?: RequestOptions,
  ): Promise<TableResource>;
  async fetchTable(
    databaseOrReference: string | TableReference,
    schema?: string,
    table?: string,
    options: RequestOptions = {},
  ): Promise<TableResource> {
    const [database, schemaName, tableName, requestOptions] = tableReferenceArguments(
      databaseOrReference,
      schema,
      table,
      options,
    );
    return this.requestJson(
      this.resourceUrl([
        "databases",
        database,
        "schemas",
        schemaName,
        "tables",
        tableName,
      ]),
      {
        method: "GET",
        signal: requestOptions.signal,
      },
    );
  }

  /** Iterates databases across all catalog pages. */
  async *iterateDatabases(
    options: CatalogListOptions = {},
  ): AsyncGenerator<DatabaseResource> {
    yield* iterateCatalogPages(
      (pageToken) => this.listDatabases({ ...options, pageToken }),
      options.pageToken,
    );
  }

  /** Iterates schemas across all catalog pages. */
  iterateSchemas(reference: SchemaCatalogListOptions): AsyncGenerator<SchemaResource>;
  iterateSchemas(
    database: string,
    options?: CatalogListOptions,
  ): AsyncGenerator<SchemaResource>;
  async *iterateSchemas(
    databaseOrReference: string | SchemaCatalogListOptions,
    options: CatalogListOptions = {},
  ): AsyncGenerator<SchemaResource> {
    const [database, listOptions] = schemaListArguments(
      databaseOrReference,
      options,
    );
    yield* iterateCatalogPages(
      (pageToken) => this.listSchemas(database, { ...listOptions, pageToken }),
      listOptions.pageToken,
    );
  }

  /** Iterates table summaries across all catalog pages. */
  iterateTables(reference: TableCatalogListOptions): AsyncGenerator<TableResourceSummary>;
  iterateTables(
    database: string,
    schema: string,
    options?: CatalogListOptions,
  ): AsyncGenerator<TableResourceSummary>;
  async *iterateTables(
    databaseOrReference: string | TableCatalogListOptions,
    schema?: string,
    options: CatalogListOptions = {},
  ): AsyncGenerator<TableResourceSummary> {
    const [database, schemaName, listOptions] = tableListArguments(
      databaseOrReference,
      schema,
      options,
    );
    yield* iterateCatalogPages(
      (pageToken) => this.listTables(database, schemaName, {
        ...listOptions,
        pageToken,
      }),
      listOptions.pageToken,
    );
  }

  /** @deprecated Use `client.table(name, location).append(ndjson)`. */
  async appendRows(
    database: string,
    schema: string,
    table: string,
    ndjson: string,
    options: RequestOptions = {},
  ): Promise<AppendRowsResult> {
    // A request that has not started cannot have an ambiguous commit outcome.
    // Keep this check outside the catch block below so the caller's abort reason
    // is preserved instead of being wrapped as AppendRowsError("unknown").
    options.signal?.throwIfAborted();
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
      const response = await this.request(url, {
        method: "POST",
        headers: { "Content-Type": "application/x-ndjson" },
        body: ndjson,
        signal: options.signal,
      });
      const result: unknown = await parseJsonResponse(response);
      if (!isAppendRowsResult(result)) {
        throw new ScopeDBError(
          "Unexpected",
          "append response has an invalid body",
          responseMetadata(response),
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
        {
          cause: error,
          httpStatus: error.httpStatus,
          requestId: error.requestId,
          retryAfterMs: error.retryAfterMs,
        },
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

  /** @deprecated Use `client.statement(scopeql).submit()`. */
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

  /** @deprecated Use `client.statementHandle(id).status()` or `.wait()`. */
  async fetchStatement(
    statementId: string,
    options: RequestOptions = {},
  ): Promise<StatementStatus> {
    const url = this.resourceUrl(["statements", statementId]);
    url.searchParams.set("format", "json");
    return this.requestJson(url, {
      method: "GET",
      signal: options.signal,
    });
  }

  /** @deprecated Use `client.statementHandle(id).cancel()`. */
  async cancelStatement(
    statementId: string,
    options: RequestOptions = {},
  ): Promise<StatementCancelResult> {
    return this.requestJson(this.resourceUrl(["statements", statementId, "cancel"]), {
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
    return parseJsonResponse<T>(response);
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
      response = await this.fetchFn(url, {
        ...init,
        cache: init.cache ?? "no-store",
        headers,
      });
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
      if (
        !Number.isSafeInteger(options.pageSize) ||
        options.pageSize < 1 ||
        options.pageSize > 1_000
      ) {
        throw new ScopeDBError(
          "ConfigInvalid",
          "catalog pageSize must be an integer from 1 to 1000",
        );
      }
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
  let message = body.length > 0
    ? body
    : response.statusText || `HTTP ${response.status}`;
  let appendPayload: AppendRowsErrorPayload | undefined;
  let requestId = response.headers.get("x-request-id") ?? undefined;
  let serverRetryable: boolean | undefined;
  try {
    const payload: unknown = JSON.parse(body);
    const parsed = parseErrorPayload(payload);
    if (parsed !== undefined) {
      message = parsed.message;
      requestId = parsed.requestId ?? requestId;
      serverRetryable = parsed.retryable;
      appendPayload = asAppendRowsErrorPayload(parsed.raw, parsed.message);
    }
  } catch {
    // Fall back to the raw response body.
  }

  const metadata = {
    ...responseMetadata(response),
    ...(requestId === undefined ? {} : { requestId }),
  };
  const error = appendPayload === undefined
    ? new ScopeDBError("Unexpected", message, metadata)
    : new AppendRowsError(appendPayload, message, metadata);
  if (error instanceof AppendRowsError && error.appendState === "unknown") {
    // Retrying an append with an unknown commit outcome can insert duplicates.
    return error.setPersistent();
  }
  const retryable = serverRetryable ??
    (response.status === 408 || response.status === 429 || response.status >= 500);
  if (retryable) {
    return error.setTemporary();
  }
  return error.setPermanent();
}

async function parseJsonResponse<T>(response: Response): Promise<T> {
  try {
    return (await response.json()) as T;
  } catch (cause) {
    throw new ScopeDBError("Unexpected", "failed to parse response body", {
      cause,
      ...responseMetadata(response),
    });
  }
}

function responseMetadata(response: Response): {
  httpStatus: number;
  requestId?: string;
  retryAfterMs?: number;
} {
  const requestId = response.headers.get("x-request-id") ?? undefined;
  return {
    httpStatus: response.status,
    ...(requestId === undefined ? {} : { requestId }),
    ...parseRetryAfter(response.headers.get("retry-after")),
  };
}

interface ParsedErrorPayload {
  message: string;
  requestId?: string;
  retryable?: boolean;
  raw: Record<string, unknown>;
}

function parseErrorPayload(value: unknown): ParsedErrorPayload | undefined {
  if (!isRecord(value)) {
    return undefined;
  }

  if (typeof value["message"] === "string") {
    return {
      message: value["message"],
      ...(typeof value["request_id"] === "string"
        ? { requestId: value["request_id"] }
        : {}),
      ...(typeof value["retryable"] === "boolean"
        ? { retryable: value["retryable"] }
        : {}),
      raw: value,
    };
  }

  const nested = value["error"];
  if (!isRecord(nested) || typeof nested["message"] !== "string") {
    return undefined;
  }
  return {
    message: nested["message"],
    ...(typeof value["request_id"] === "string"
      ? { requestId: value["request_id"] }
      : {}),
    ...(typeof nested["retryable"] === "boolean"
      ? { retryable: nested["retryable"] }
      : {}),
    raw: value,
  };
}

function asAppendRowsErrorPayload(
  value: Record<string, unknown>,
  message: string,
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
    message,
    append_state: appendState,
    row_errors: rowErrors ?? [],
    row_errors_truncated: rowErrorsTruncated ?? false,
  };
}

function parseRetryAfter(value: string | null): { retryAfterMs?: number } {
  if (value === null) {
    return {};
  }
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) {
    return { retryAfterMs: Math.round(seconds * 1_000) };
  }
  const at = Date.parse(value);
  if (!Number.isNaN(at)) {
    return { retryAfterMs: Math.max(0, at - Date.now()) };
  }
  return {};
}

function schemaListArguments(
  databaseOrReference: string | SchemaCatalogListOptions,
  options: CatalogListOptions,
): [string, CatalogListOptions] {
  if (typeof databaseOrReference === "string") {
    return [databaseOrReference, options];
  }
  const { database, ...listOptions } = databaseOrReference;
  return [database, listOptions];
}

function tableListArguments(
  databaseOrReference: string | TableCatalogListOptions,
  schema: string | undefined,
  options: CatalogListOptions,
): [string, string, CatalogListOptions] {
  if (typeof databaseOrReference === "string") {
    if (schema === undefined) {
      throw new ScopeDBError("ConfigInvalid", "schema is required");
    }
    return [databaseOrReference, schema, options];
  }
  const { database, schema: schemaName, ...listOptions } = databaseOrReference;
  return [database, schemaName, listOptions];
}

function schemaReferenceArguments(
  databaseOrReference: string | SchemaReference,
  schema: string | undefined,
  options: RequestOptions,
): [string, string, RequestOptions] {
  if (typeof databaseOrReference === "string") {
    if (schema === undefined) {
      throw new ScopeDBError("ConfigInvalid", "schema is required");
    }
    return [databaseOrReference, schema, options];
  }
  const { database, schema: schemaName, ...requestOptions } = databaseOrReference;
  return [database, schemaName, requestOptions];
}

function tableReferenceArguments(
  databaseOrReference: string | TableReference,
  schema: string | undefined,
  table: string | undefined,
  options: RequestOptions,
): [string, string, string, RequestOptions] {
  if (typeof databaseOrReference === "string") {
    if (schema === undefined || table === undefined) {
      throw new ScopeDBError("ConfigInvalid", "schema and table are required");
    }
    return [databaseOrReference, schema, table, options];
  }
  const {
    database,
    schema: schemaName,
    table: tableName,
    ...requestOptions
  } = databaseOrReference;
  return [database, schemaName, tableName, requestOptions];
}

async function* iterateCatalogPages<T>(
  fetchPage: (pageToken: string | undefined) => Promise<CatalogPage<T>>,
  initialPageToken: string | undefined,
): AsyncGenerator<T> {
  let pageToken = initialPageToken;
  const seenTokens = new Set<string>(
    initialPageToken === undefined ? [] : [initialPageToken],
  );
  for (;;) {
    const page = await fetchPage(pageToken);
    yield* page.items;
    const nextPageToken = page.next_page_token;
    if (nextPageToken === undefined) {
      return;
    }
    if (seenTokens.has(nextPageToken)) {
      throw new ScopeDBError(
        "Unexpected",
        "catalog pagination returned a repeated page token",
      );
    }
    seenTokens.add(nextPageToken);
    pageToken = nextPageToken;
  }
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
