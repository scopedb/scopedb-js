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

import {
  AppendRowsError,
  asScopeDBError,
  ScopeDBError,
  writeErrorDetailsFromCause,
} from "./errors.js";
import type {
  AppendRowError,
  AppendRowsErrorPayload,
  AppendRowsResult,
  StatementErrorDetails,
  StatementRequest,
  StatementResultSet,
  StatementSnapshot,
} from "./protocol.js";
import {
  Catalog,
  type CatalogOptions,
  validateCatalogResponse,
} from "./catalog.js";
import type { ResultSet } from "./result.js";
import { isDataType } from "./protocol.js";
import { Statement, StatementHandle } from "./statement.js";
import type { WaitOptions } from "./statement.js";
import {
  byteLength,
  MAX_APPEND_BODY_BYTES,
  MAX_APPEND_ROWS,
  writeTimeoutConfig,
} from "./stream-internals.js";
import { Table, type TableOptions } from "./table.js";

export interface RequestOptions {
  signal?: AbortSignal;
}

export interface WriteOptions extends RequestOptions {
  /** Per-request timeout in milliseconds; undefined inherits, null disables the SDK deadline. */
  timeoutMs?: number | null;
}

export interface ClientOptions {
  fetch?: typeof globalThis.fetch;
  /**
   * Timeout for each table append or catalog mutation, including its
   * response body, in milliseconds. Defaults to 30,000; null disables the SDK deadline, positive integers override it.
   * The budget starts after local encoding, compression, and request preparation.
   * Stream attemptTimeoutMs settings override this default. A timeout does not
   * roll back the write; its commit outcome can be unknown.
   */
  writeTimeoutMs?: number | null;
  /** Default headers sent with every request. */
  headers?: HeadersInit;
  /**
   * ScopeDB API key. It is sent as a Bearer credential and must only be used
   * from trusted server-side code. It takes precedence over an
   * `Authorization` value in `headers`.
   */
  apiKey?: string;
}

export class Client {
  private readonly endpoint: URL;
  private readonly fetchFn: typeof globalThis.fetch;
  private readonly defaultHeaders: Headers;
  private readonly writeTimeoutMs: number | null;

  constructor(endpoint: string | URL, options: ClientOptions = {}) {
    this.writeTimeoutMs = writeTimeoutConfig(
      "writeTimeoutMs",
      options.writeTimeoutMs === undefined ? 30_000 : options.writeTimeoutMs,
    );
    try {
      this.endpoint = normalizeEndpoint(endpoint);
    } catch (cause) {
      throw new ScopeDBError("ConfigInvalid", "failed to parse endpoint", {
        cause,
      });
    }

    const fetchFn = options.fetch ?? globalThis.fetch;
    // Some Web runtimes require fetch to be called without an arbitrary
    // receiver. Calling a stored function as `this.fetchFn()` would otherwise
    // pass the Client instance as `this` and fail with an illegal invocation.
    this.fetchFn = (input, init) => fetchFn(input, init);
    this.defaultHeaders = new Headers(options.headers);
    const apiKey = options.apiKey;
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

  catalog(): Catalog {
    return new Catalog((method, path, definition, options) =>
      this.catalogRequest(method, path, definition, options)
    );
  }

  private async catalogRequest(
    method: string,
    path: string[],
    definition: unknown,
    options: CatalogOptions,
  ): Promise<{ body: Record<string, unknown>; etag: string }> {
    const url = this.resourceUrl(["catalog", ...path]);
    if (options.dryRun) url.searchParams.set("dry_run", "true");
    if (options.allowDestructive) {
      url.searchParams.set("allow_destructive", "true");
    }
    const headers = new Headers();
    if (options.ifMatch !== undefined) headers.set("If-Match", options.ifMatch);
    const body = definition === undefined
      ? undefined
      : JSON.stringify(definition);
    options.signal?.throwIfAborted();
    // ScopeDB 0.6 Catalog handlers decode JSON directly without request decompression.
    const prepared = this.prepareRequest(url, { method, headers, body });
    const mutation = method !== "GET";
    const writing = mutation && !options.dryRun;
    const controller = new AbortController();
    const abort = () => controller.abort(options.signal?.reason);
    const timeoutMs = mutation
      ? writeTimeoutConfig(
        "timeoutMs",
        options.timeoutMs === undefined
          ? this.writeTimeoutMs
          : options.timeoutMs,
      )
      : null;
    options.signal?.addEventListener("abort", abort, { once: true });
    const timer = timeoutMs === null
      ? undefined
      : setTimeout(() =>
        controller.abort(
          new DOMException("catalog request timed out", "TimeoutError"),
        ), timeoutMs);
    let rejectAborted!: () => void;
    const aborted = new Promise<never>((_resolve, reject) => {
      rejectAborted = () => reject(controller.signal.reason);
      controller.signal.addEventListener("abort", rejectAborted, {
        once: true,
      });
    });
    let response: Response | undefined;
    let failure: ScopeDBError | undefined;
    try {
      return await Promise.race([
        (async () => {
          response = await this.sendRequest(prepared.url, {
            ...prepared.init,
            signal: controller.signal,
          });
          if (!response.ok) {
            failure = await responseToError(response);
            if (writing) {
              if (failure.code === "catalog_commit_outcome_unknown") {
                failure.setPersistent();
              } else if (
                failure.code === undefined &&
                (response.status === 408 || response.status === 429 ||
                  response.status >= 500)
              ) {
                failure = new ScopeDBError(
                  "CatalogFailed",
                  "catalog commit outcome is unknown",
                  {
                    cause: failure,
                    code: "catalog_commit_outcome_unknown",
                    ...responseMetadata(response),
                  },
                ).setPersistent();
              }
            }
            throw failure;
          }
          const value: unknown = await response.json();
          const etag = response.headers.get("etag");
          if (!isRecord(value) || !etag) {
            throw new Error("invalid catalog response or missing ETag");
          }
          validateCatalogResponse(value, path, method);
          return { body: value, etag };
        })(),
        aborted,
      ]);
    } catch (cause) {
      if (failure !== undefined && cause === failure) throw cause;
      throw new ScopeDBError(
        "CatalogFailed",
        writing
          ? "catalog commit outcome is unknown"
          : "failed to read catalog",
        {
          cause,
          code: writing ? "catalog_commit_outcome_unknown" : undefined,
          status: writing
            ? "persistent"
            : cause instanceof ScopeDBError
            ? cause.status()
            : "permanent",
          ...(response === undefined ? {} : responseMetadata(response)),
        },
      );
    } finally {
      clearTimeout(timer);
      options.signal?.removeEventListener("abort", abort);
      controller.signal.removeEventListener("abort", rejectAborted);
    }
  }

  /**
   * Appends at most 8 MiB of uncompressed newline-delimited JSON.
   * @internal Transport used by Table and AppendStream.
   */
  async appendRows(
    database: string,
    schema: string,
    table: string,
    ndjson: string,
    options: WriteOptions = {},
  ): Promise<AppendRowsResult> {
    options.signal?.throwIfAborted();
    const uncompressedBytes = byteLength(ndjson);
    if (uncompressedBytes > MAX_APPEND_BODY_BYTES) {
      const message =
        `append payload requires ${uncompressedBytes} bytes, exceeds the ${MAX_APPEND_BODY_BYTES}-byte append limit`;
      throw new AppendRowsError(
        {
          message,
          status: "rejected",
          row_errors: [],
          row_errors_truncated: false,
        },
        message,
      );
    }
    const expectedRows = countNDJSONRows(ndjson);
    if (expectedRows > MAX_APPEND_ROWS) {
      const message =
        `append payload contains ${expectedRows} rows, exceeds the ${MAX_APPEND_ROWS}-row append limit`;
      throw new AppendRowsError({
        message,
        status: "rejected",
        row_errors: [],
        row_errors_truncated: false,
      }, message);
    }
    const { body } = await gzipJsonRequestBody(ndjson);
    const url = this.resourceUrl([
      "catalog",
      "databases",
      database,
      "schemas",
      schema,
      "tables",
      table,
      "ingest",
    ]);
    return this.appendRequest(
      url,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/x-ndjson",
          "Content-Encoding": "gzip",
        },
        body,
      },
      options,
      (value) => {
        const wire = expectResponse(
          value,
          isAppendRowsResult,
          "append response",
        );
        const result: AppendRowsResult = {
          status: wire.status,
          num_rows_inserted: wire.num_rows_inserted,
        };
        if (result.num_rows_inserted !== expectedRows) {
          throw new ScopeDBError(
            "Unexpected",
            `table append response reported ${result.num_rows_inserted} inserted rows for a ${expectedRows}-row request`,
          );
        }
        return result;
      },
    );
  }

  /** @internal Transport used by Statement.submit(). */
  async submitStatement(
    request: StatementRequest,
    options: RequestOptions = {},
  ): Promise<StatementSnapshot> {
    try {
      return expectResponse(
        await this.requestJson("v1/statements", {
          method: "POST",
          body: JSON.stringify(request),
          signal: options.signal,
        }),
        isStatementSnapshot,
        "statement response",
      );
    } catch (cause) {
      if (options.signal?.aborted) throw cause;
      // A transport failure or an unstructured gateway response cannot prove
      // that submission was rejected. Replaying a mutating statement may run it twice.
      if (
        cause instanceof ScopeDBError &&
        ((cause.httpStatus === undefined && cause.isTemporary()) ||
          (cause.code === undefined && cause.httpStatus !== undefined &&
            (cause.httpStatus === 408 || cause.httpStatus === 429 ||
              cause.httpStatus >= 500)))
      ) {
        throw new ScopeDBError("Unexpected", "statement submission outcome is unknown", {
          cause,
          code: "statement_submission_outcome_unknown",
          status: "persistent",
          httpStatus: cause.httpStatus,
          requestId: cause.requestId,
          retryAfterMs: cause.retryAfterMs,
          serverRetryable: cause.serverRetryable,
          details: cause.details,
        });
      }
      throw cause;
    }
  }

  /** @internal Transport used by StatementHandle.refresh(). */
  async fetchStatement(
    statementId: string,
    options: RequestOptions = {},
  ): Promise<StatementSnapshot> {
    const url = this.resourceUrl(["statements", statementId]);
    url.searchParams.set("format", "json");
    return expectResponse(
      await this.requestJson(url, {
        method: "GET",
        signal: options.signal,
      }),
      isStatementSnapshot,
      "statement response",
    );
  }

  /** @internal Transport used by StatementHandle.cancel(). */
  async cancelStatement(
    statementId: string,
    options: RequestOptions = {},
  ): Promise<StatementSnapshot> {
    const url = this.resourceUrl(["statements", statementId, "cancel"]);
    url.searchParams.set("format", "json");
    return expectResponse(
      await this.requestJson(url, {
        method: "POST",
        signal: options.signal,
      }),
      isStatementSnapshot,
      "statement cancellation response",
    );
  }

  private async requestJson<T>(
    path: string | URL,
    init: RequestInit,
  ): Promise<T> {
    return withCallerAbort(async () => {
      const prepared = await this.prepareJsonRequest(init);
      init.signal?.throwIfAborted();
      const response = await this.request(path, prepared);
      if (!response.ok) {
        throw await responseToError(response);
      }
      return parseJsonResponse<T>(response);
    }, init.signal);
  }

  private async prepareJsonRequest(init: RequestInit): Promise<RequestInit> {
    if (init.body === undefined) {
      return init;
    }
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
    return { ...init, body, headers };
  }

  private async appendRequest<T>(
    path: string | URL,
    init: RequestInit,
    options: WriteOptions,
    decode: (value: unknown) => T,
  ): Promise<T> {
    // Cancellation before sending cannot have an ambiguous commit outcome.
    const signal = options.signal;
    signal?.throwIfAborted();
    const timeoutMs = writeTimeoutConfig(
      "timeoutMs",
      options.timeoutMs === undefined ? this.writeTimeoutMs : options.timeoutMs,
    );
    const prepared = this.prepareRequest(path, init);
    signal?.throwIfAborted();
    const controller = new AbortController();
    const abort = () =>
      controller.abort(
        new ScopeDBError(
          "Unexpected",
          "write request cancelled; commit outcome is unknown",
          {
            cause: signal?.reason,
            writeDetails: {
              reason: signal?.reason instanceof DOMException &&
                  signal.reason.name === "TimeoutError"
                ? "timeout"
                : "cancelled",
              outcome: "unknown",
            },
          },
        ).setPersistent(),
      );
    signal?.addEventListener("abort", abort, { once: true });
    const timer = timeoutMs === null
      ? undefined
      : setTimeout(() =>
        controller.abort(
          new ScopeDBError(
            "Unexpected",
            "write request timed out; commit outcome is unknown",
            {
              cause: new DOMException(
                "write request timed out",
                "TimeoutError",
              ),
              writeDetails: { reason: "timeout", outcome: "unknown" },
            },
          ).withContext("write_timeout_ms", timeoutMs).setPersistent(),
        ), timeoutMs);
    let rejectAborted!: () => void;
    const aborted = new Promise<never>((_resolve, reject) => {
      rejectAborted = () => reject(controller.signal.reason);
      controller.signal.addEventListener("abort", rejectAborted, {
        once: true,
      });
    });
    let response: Response | undefined;
    let responseFailure: ScopeDBError | undefined;
    try {
      return await Promise.race([
        (async () => {
          response = await this.sendRequest(prepared.url, {
            ...prepared.init,
            signal: controller.signal,
          });
          if (!response.ok) {
            responseFailure = await responseToError(response);
            throw responseFailure;
          }
          return decode(await parseJsonResponse<unknown>(response));
        })(),
        aborted,
      ]);
    } catch (cause) {
      const error = asScopeDBError(
        "Unexpected",
        "failed to append rows",
        cause,
      );
      const writeDetails = error.writeDetails ??
        writeErrorDetailsFromCause(error);
      if (
        responseFailure !== undefined && cause === responseFailure &&
        writeDetails === undefined
      ) {
        if (error instanceof AppendRowsError) {
          throw error;
        }
      }
      const metadata = response === undefined
        ? undefined
        : responseMetadata(response);
      const errorOptions = {
        cause: error,
        code: error.code,
        serverRetryable: error.serverRetryable,
        details: error.details,
        httpStatus: error.httpStatus ?? metadata?.httpStatus,
        requestId: error.requestId ?? metadata?.requestId,
        retryAfterMs: error.retryAfterMs ?? metadata?.retryAfterMs,
        writeDetails,
      };
      const unknown = new AppendRowsError(
        {
          message: error.message,
          status: "unknown",
          row_errors: [],
          row_errors_truncated: false,
        },
        error.message,
        errorOptions,
      );
      for (const [key, value] of error.context()) {
        unknown.withContext(key, value);
      }
      throw unknown.setPersistent();
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
      controller.signal.removeEventListener("abort", rejectAborted);
    }
  }

  private async request(
    path: string | URL,
    init: RequestInit,
  ): Promise<Response> {
    const prepared = this.prepareRequest(path, init);
    return this.sendRequest(prepared.url, prepared.init);
  }

  private prepareRequest(
    path: string | URL,
    init: RequestInit,
  ): { url: URL; init: RequestInit } {
    const headers = new Headers(this.defaultHeaders);
    headers.set("Accept", "application/json");
    headers.set("X-ScopeDB-Client", "scopedb-js");
    if (init.body !== undefined) {
      headers.set("Content-Type", "application/json");
    }
    const providedHeaders = new Headers(init.headers);
    providedHeaders.forEach((value, key) => headers.set(key, value));

    const url = path instanceof URL ? path : this.makeUrl(path);

    return { url, init: { ...init, cache: init.cache ?? "no-store", headers } };
  }

  private async sendRequest(url: URL, init: RequestInit): Promise<Response> {
    try {
      return await this.fetchFn(url, init);
    } catch (cause) {
      if (init.signal?.aborted === true) {
        throw init.signal.reason;
      }
      throw asScopeDBError(
        "Unexpected",
        `failed to send request to ${url}`,
        cause,
      ).setTemporary();
    }
  }

  private makeUrl(path: string): URL {
    return new URL(path, this.endpoint);
  }

  private resourceUrl(segments: readonly string[]): URL {
    if (
      segments.some((segment) =>
        segment === "" || segment === "." || segment === ".."
      )
    ) {
      throw new ScopeDBError(
        "ConfigInvalid",
        "resource names must not be empty or dot segments",
      );
    }
    return this.makeUrl(["v1", ...segments].map(encodeURIComponent).join("/"));
  }
}

async function withCallerAbort<T>(
  operation: () => Promise<T>,
  signal: AbortSignal | null | undefined,
): Promise<T> {
  if (signal == null) return operation();
  signal.throwIfAborted();
  let onAbort!: () => void;
  const aborted = new Promise<never>((_resolve, reject) => {
    onAbort = () => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
  });
  try {
    return await Promise.race([operation(), aborted]);
  } catch (cause) {
    if (signal.aborted) throw signal.reason;
    throw cause;
  } finally {
    signal.removeEventListener("abort", onAbort);
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
  const retryableByStatus = response.status === 408 ||
    response.status === 429 ||
    response.status >= 500;
  let body: string;
  try {
    body = await response.text();
  } catch (cause) {
    return new ScopeDBError("Unexpected", "failed to read response body", {
      cause,
      ...responseMetadata(response),
      status: retryableByStatus ? "temporary" : "permanent",
    });
  }
  let message = body.length > 0
    ? body
    : response.statusText || `HTTP ${response.status}`;
  let appendPayload: AppendRowsErrorPayload | undefined;
  let requestId = response.headers.get("x-request-id") ?? undefined;
  let serverRetryable: boolean | undefined;
  let code: string | undefined;
  let details: unknown;
  try {
    const payload: unknown = JSON.parse(body);
    const parsed = parseErrorPayload(payload);
    if (parsed !== undefined) {
      message = parsed.message;
      code = typeof parsed.raw["code"] === "string"
        ? parsed.raw["code"]
        : undefined;
      details = parsed.raw["details"];
      requestId = parsed.requestId ?? requestId;
      serverRetryable = parsed.retryable;
      appendPayload = asAppendRowsErrorPayload(parsed.raw, parsed.message);
    }
  } catch {
    // Fall back to the raw response body.
  }

  const metadata = {
    code,
    details,
    serverRetryable,
    ...responseMetadata(response),
    ...(requestId === undefined ? {} : { requestId }),
  };
  const error = appendPayload === undefined
    ? new ScopeDBError("Unexpected", message, metadata)
    : new AppendRowsError(appendPayload, message, metadata);
  if (error instanceof AppendRowsError && error.outcome === "unknown") {
    // Retrying an append with an unknown commit outcome can insert duplicates.
    return error.setPersistent();
  }
  const retryable = serverRetryable ?? retryableByStatus;
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
    raw: nested,
  };
}

function asAppendRowsErrorPayload(
  value: Record<string, unknown>,
  message: string,
): AppendRowsErrorPayload | undefined {
  const details = value["details"];
  if (!isRecord(details)) return undefined;
  const outcome = details["status"];
  if (outcome !== "rejected" && outcome !== "unknown") {
    return undefined;
  }

  const rowErrors = details["row_errors"];
  if (rowErrors !== undefined && !isAppendRowErrors(rowErrors)) {
    return undefined;
  }
  const rowErrorsTruncated = details["row_errors_truncated"];
  if (
    rowErrorsTruncated !== undefined && typeof rowErrorsTruncated !== "boolean"
  ) {
    return undefined;
  }

  return {
    message,
    status: outcome,
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

function isStatementSnapshot(value: unknown): value is StatementSnapshot {
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
      return isStatementErrorDetails(value["error"]);
    case "cancelled":
      return true;
    default:
      return false;
  }
}

const STATEMENT_PROGRESS_FIELDS = [
  "compute_seconds",
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
  return isRecord(value) &&
    STATEMENT_PROGRESS_FIELDS.every((field) =>
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
  return rows.length === value["metadata"]["num_rows"] &&
    rows.every((row) =>
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

function isStatementErrorDetails(
  value: unknown,
): value is StatementErrorDetails {
  return isRecord(value) &&
    typeof value["code"] === "string" &&
    typeof value["message"] === "string";
}

function countNDJSONRows(ndjson: string): number {
  return ndjson.split("\n").filter((line) => line.trim().length > 0).length;
}

function isAppendRowsResult(
  value: unknown,
): value is { status: "committed"; num_rows_inserted: number } {
  return isRecord(value) &&
    value["status"] === "committed" &&
    typeof value["num_rows_inserted"] === "number" &&
    Number.isSafeInteger(value["num_rows_inserted"]) &&
    value["num_rows_inserted"] >= 0;
}
