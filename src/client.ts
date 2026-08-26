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
  DataType,
  DatabaseResource,
  IngestRequest,
  IngestResult,
  SchemaResource,
  StatementCancelResult,
  StatementErrorDetails,
  StatementRequest,
  StatementResultSet,
  StatementStatus,
  TableColumnSpec,
  TableDistinctSpec,
  TableResource,
  TableResourceSummary,
} from "./protocol.js";
import type { ResultSet } from "./result.js";
import { Statement, StatementHandle } from "./statement.js";
import type { WaitOptions } from "./statement.js";
import { MAX_APPEND_BODY_BYTES, byteLength } from "./stream-internals.js";
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
    return expectCatalogPage(
      await this.requestJson(this.catalogUrl(["databases"], options), {
        method: "GET",
        signal: options.signal,
      }),
      isDatabaseResource,
      "database catalog page",
    );
  }

  async fetchDatabase(
    database: string,
    options: RequestOptions = {},
  ): Promise<DatabaseResource> {
    return expectResponse(
      await this.requestJson(this.resourceUrl(["databases", database]), {
        method: "GET",
        signal: options.signal,
      }),
      isDatabaseResource,
      "database resource",
    );
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
    return expectCatalogPage(
      await this.requestJson(
        this.catalogUrl(["databases", database, "schemas"], listOptions),
        {
          method: "GET",
          signal: listOptions.signal,
        },
      ),
      isSchemaResource,
      "schema catalog page",
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
    return expectResponse(
      await this.requestJson(
        this.resourceUrl(["databases", database, "schemas", schemaName]),
        {
          method: "GET",
          signal: requestOptions.signal,
        },
      ),
      isSchemaResource,
      "schema resource",
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
    return expectCatalogPage(
      await this.requestJson(
        this.catalogUrl(
          ["databases", database, "schemas", schemaName, "tables"],
          listOptions,
        ),
        {
          method: "GET",
          signal: listOptions.signal,
        },
      ),
      isTableResourceSummary,
      "table catalog page",
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
    return expectResponse(
      await this.requestJson(
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
      ),
      isTableResource,
      "table resource",
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

  /**
   * Appends at most 8 MiB of uncompressed newline-delimited JSON.
   * @deprecated Use `client.table(name, location).append(ndjson)`.
   */
  async appendRows(
    database: string,
    schema: string,
    table: string,
    ndjson: string,
    options: RequestOptions = {},
  ): Promise<AppendRowsResult> {
    options.signal?.throwIfAborted();
    const uncompressedBytes = byteLength(ndjson);
    if (uncompressedBytes > MAX_APPEND_BODY_BYTES) {
      const message =
        `append payload requires ${uncompressedBytes} bytes, exceeds the ${MAX_APPEND_BODY_BYTES}-byte append limit`;
      throw new AppendRowsError(
        {
          message,
          append_state: "rejected",
          row_errors: [],
          row_errors_truncated: false,
        },
        message,
      );
    }
    const expectedRows = countNDJSONRows(ndjson);
    const { body } = await gzipJsonRequestBody(ndjson);
    return this.sendAppendRows(database, schema, table, body, expectedRows, options);
  }

  private async sendAppendRows(
    database: string,
    schema: string,
    table: string,
    body: ArrayBuffer,
    expectedRows: number,
    options: RequestOptions,
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
      const headers = new Headers({
        "Content-Type": "application/x-ndjson",
        "Content-Encoding": "gzip",
      });
      const response = await this.request(url, {
        method: "POST",
        headers,
        body,
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
      if (result.num_rows_inserted !== expectedRows) {
        const message =
          `table append response reported ${result.num_rows_inserted} inserted rows for a ${expectedRows}-row request`;
        throw new AppendRowsError(
          {
            message,
            append_state: "unknown",
            row_errors: [],
            row_errors_truncated: false,
          },
          message,
          responseMetadata(response),
        ).setPersistent();
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
    return expectResponse(
      await this.requestJson("v1/statements", {
        method: "POST",
        body: JSON.stringify(request),
        signal: options.signal,
      }),
      isStatementStatus,
      "statement response",
    );
  }

  /** @deprecated Use `client.statementHandle(id).status()` or `.wait()`. */
  async fetchStatement(
    statementId: string,
    options: RequestOptions = {},
  ): Promise<StatementStatus> {
    const url = this.resourceUrl(["statements", statementId]);
    url.searchParams.set("format", "json");
    return expectResponse(
      await this.requestJson(url, {
        method: "GET",
        signal: options.signal,
      }),
      isStatementStatus,
      "statement response",
    );
  }

  /** @deprecated Use `client.statementHandle(id).cancel()`. */
  async cancelStatement(
    statementId: string,
    options: RequestOptions = {},
  ): Promise<StatementCancelResult> {
    return expectResponse(
      await this.requestJson(this.resourceUrl(["statements", statementId, "cancel"]), {
        method: "POST",
        signal: options.signal,
      }),
      isStatementCancelResult,
      "statement cancellation response",
    );
  }

  async ingest(
    request: IngestRequest,
    options: RequestOptions = {},
  ): Promise<IngestResult> {
    return expectResponse(
      await this.requestJson("v1/ingest", {
        method: "POST",
        body: JSON.stringify(request),
        signal: options.signal,
      }),
      isIngestResult,
      "ingest response",
    );
  }

  private async requestJson<T>(
    path: string | URL,
    init: RequestInit,
  ): Promise<T> {
    let requestInit = init;
    if (init.body !== undefined) {
      init.signal?.throwIfAborted();
      if (typeof init.body !== "string") {
        throw new ScopeDBError(
          "Unexpected",
          "JSON request body must be a string before compression",
        );
      }

      const { body, uncompressedBytes } = await gzipJsonRequestBody(init.body);
      const headers = new Headers(init.headers);
      headers.set("Content-Encoding", "gzip");
      headers.set(
        "X-ScopeDB-Uncompressed-Content-Length",
        String(uncompressedBytes),
      );
      requestInit = { ...init, body, headers };
    }

    const response = await this.request(path, requestInit);
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

async function gzipJsonRequestBody(
  body: string,
): Promise<{ body: ArrayBuffer; uncompressedBytes: number }> {
  try {
    const uncompressed = new Blob([body]);
    const compressed = uncompressed
      .stream()
      .pipeThrough(new CompressionStream("gzip"));
    return {
      body: await new Response(compressed).arrayBuffer(),
      uncompressedBytes: uncompressed.size,
    };
  } catch (cause) {
    throw new ScopeDBError(
      "Unexpected",
      "failed to compress request body with gzip",
      { cause },
    );
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

function expectResponse<T>(
  value: unknown,
  validate: (value: unknown) => value is T,
  responseName: string,
): T {
  if (!validate(value)) {
    throw new ScopeDBError("Unexpected", `${responseName} has an invalid body`);
  }
  return value;
}

function expectCatalogPage<T>(
  value: unknown,
  validateItem: (value: unknown) => value is T,
  responseName: string,
): CatalogPage<T> {
  if (!isRecord(value) || !Array.isArray(value["items"])) {
    throw new ScopeDBError("Unexpected", `${responseName} has an invalid body`);
  }
  const items = value["items"];
  if (!items.every(validateItem)) {
    throw new ScopeDBError("Unexpected", `${responseName} has an invalid body`);
  }
  const nextPageToken = value["next_page_token"];
  if (
    nextPageToken !== undefined &&
    nextPageToken !== null &&
    typeof nextPageToken !== "string"
  ) {
    throw new ScopeDBError("Unexpected", `${responseName} has an invalid body`);
  }
  return {
    items,
    ...(typeof nextPageToken === "string"
      ? { next_page_token: nextPageToken }
      : {}),
  };
}

function isDatabaseResource(value: unknown): value is DatabaseResource {
  return isRecord(value) &&
    typeof value["name"] === "string" &&
    isNullableString(value["comment"]);
}

function isSchemaResource(value: unknown): value is SchemaResource {
  return isRecord(value) &&
    typeof value["database"] === "string" &&
    typeof value["name"] === "string" &&
    isNullableString(value["comment"]);
}

function isTableResourceSummary(value: unknown): value is TableResourceSummary {
  return isRecord(value) &&
    typeof value["database"] === "string" &&
    typeof value["schema"] === "string" &&
    typeof value["name"] === "string" &&
    isNullableString(value["comment"]);
}

function isTableResource(value: unknown): value is TableResource {
  if (!isRecord(value) || !isTableResourceSummary(value)) {
    return false;
  }
  return (
    Array.isArray(value["columns"]) &&
    value["columns"].every(isTableColumnSpec) &&
    isStringArray(value["partition_by"]) &&
    isStringArray(value["cluster_by"]) &&
    isTableDistinctSpec(value["distinct_on"]) &&
    (value["data_retention_days"] === null ||
      Number.isInteger(value["data_retention_days"]))
  );
}

function isTableColumnSpec(value: unknown): value is TableColumnSpec {
  return isRecord(value) &&
    typeof value["name"] === "string" &&
    isCanonicalDataType(value["data_type"]) &&
    isNullableString(value["comment"]);
}

function isTableDistinctSpec(value: unknown): value is TableDistinctSpec {
  return isRecord(value) &&
    isStringArray(value["on"]) &&
    isStringArray(value["by"]);
}

function isStatementStatus(value: unknown): value is StatementStatus {
  if (
    !isRecord(value) ||
    typeof value["statement_id"] !== "string" ||
    typeof value["created_at"] !== "string" ||
    !isStatementProgress(value["progress"])
  ) {
    return false;
  }

  switch (value["status"]) {
    case "pending":
    case "running":
      return true;
    case "finished":
      return isStatementResultSet(value["result_set"]);
    case "failed":
      return typeof value["message"] === "string" &&
        (value["error"] === undefined || isStatementErrorDetails(value["error"]));
    case "cancelled":
      return typeof value["message"] === "string";
    default:
      return false;
  }
}

const STATEMENT_PROGRESS_FIELDS = [
  "total_percentage",
  "nanos_from_submitted",
  "nanos_from_started",
  "total_stages",
  "total_partitions",
  "total_rows",
  "total_compressed_bytes",
  "total_uncompressed_bytes",
  "scanned_stages",
  "scanned_partitions",
  "scanned_rows",
  "scanned_compressed_bytes",
  "scanned_uncompressed_bytes",
  "skipped_partitions",
  "skipped_rows",
  "skipped_compressed_bytes",
  "skipped_uncompressed_bytes",
] as const;

function isStatementProgress(value: unknown): boolean {
  return isRecord(value) && STATEMENT_PROGRESS_FIELDS.every((field) =>
    typeof value[field] === "number" && Number.isFinite(value[field])
  );
}

function isStatementResultSet(value: unknown): value is StatementResultSet {
  if (
    !isRecord(value) ||
    value["format"] !== "json" ||
    !isRecord(value["metadata"]) ||
    !Array.isArray(value["metadata"]["fields"]) ||
    !value["metadata"]["fields"].every(isFieldSchemaPayload) ||
    !Number.isSafeInteger(value["metadata"]["num_rows"]) ||
    (value["metadata"]["num_rows"] as number) < 0 ||
    !Array.isArray(value["rows"])
  ) {
    return false;
  }
  const fields = value["metadata"]["fields"];
  const rows = value["rows"];
  return rows.length === value["metadata"]["num_rows"] && rows.every((row) =>
    Array.isArray(row) &&
    row.length === fields.length &&
    row.every((cell) => cell === null || typeof cell === "string")
  );
}

function isFieldSchemaPayload(value: unknown): boolean {
  return isRecord(value) &&
    typeof value["name"] === "string" &&
    isDataType(value["data_type"]);
}

function isStatementErrorDetails(value: unknown): value is StatementErrorDetails {
  return isRecord(value) &&
    typeof value["code"] === "string" &&
    typeof value["message"] === "string";
}

function isStatementCancelResult(value: unknown): value is StatementCancelResult {
  return isRecord(value) &&
    typeof value["statement_id"] === "string" &&
    typeof value["created_at"] === "string" &&
    typeof value["message"] === "string" &&
    (value["status"] === "finished" ||
      value["status"] === "failed" ||
      value["status"] === "cancelled");
}

function isIngestResult(value: unknown): value is IngestResult {
  return isRecord(value) &&
    Number.isSafeInteger(value["num_rows_inserted"]) &&
    (value["num_rows_inserted"] as number) >= 0;
}

const DATA_TYPES = new Set<DataType>([
  "int",
  "uint",
  "u_int",
  "float",
  "timestamp",
  "interval",
  "boolean",
  "string",
  "binary",
  "array",
  "object",
  "any",
  "null",
]);

function isDataType(value: unknown): value is DataType {
  return typeof value === "string" && DATA_TYPES.has(value as DataType);
}

function isCanonicalDataType(
  value: unknown,
): value is Exclude<DataType, "u_int"> {
  return isDataType(value) && value !== "u_int";
}

function isNullableString(value: unknown): value is string | null {
  return value === null || typeof value === "string";
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string");
}

function countNDJSONRows(ndjson: string): number {
  return ndjson.split("\n").filter((line) => line.trim().length > 0).length;
}

function isAppendRowsResult(value: unknown): value is AppendRowsResult {
  return isRecord(value) &&
    value["append_state"] === "committed" &&
    typeof value["num_rows_inserted"] === "number" &&
    Number.isSafeInteger(value["num_rows_inserted"]) &&
    value["num_rows_inserted"] >= 0;
}
