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

import type {
  AppendRowError,
  AppendState,
  AppendRowsErrorPayload,
  StatementErrorDetails,
} from "./protocol.js";

export type ErrorKind =
  | "Unexpected"
  | "CatalogFailed"
  | "ConfigInvalid"
  /** The statement execution was rejected by the server (failed or cancelled in-band). */
  | "StatementFailed"
  /** The table append was rejected or its commit outcome is unknown. */
  | "AppendRowsFailed";
export type ErrorStatus = "permanent" | "temporary" | "persistent";

/** The client-side reason an in-flight write was interrupted. */
export type WriteErrorReason = "timeout" | "cancelled";

/** An unknown write outcome may have committed and must not be automatically replayed. */
export interface WriteErrorDetails {
  readonly reason: WriteErrorReason;
  readonly outcome: AppendState;
}

export interface ScopeDBErrorOptions {
  code?: string;
  details?: unknown;
  cause?: unknown;
  status?: ErrorStatus;
  /** HTTP response status, when the error came from the server. */
  httpStatus?: number;
  /** Request identifier returned by ScopeDB or an intermediary. */
  requestId?: string;
  /** Delay suggested by `Retry-After`, in milliseconds. */
  retryAfterMs?: number;
  /** Structured server failure for a failed statement. */
  statementDetails?: StatementErrorDetails;
  /** Client timeout or cancellation details after sending began. */
  writeDetails?: WriteErrorDetails;
}

export class ScopeDBError extends Error {
  readonly kind: ErrorKind;
  readonly code?: string;
  readonly details?: unknown;
  readonly httpStatus?: number;
  readonly requestId?: string;
  readonly retryAfterMs?: number;
  readonly statementDetails?: StatementErrorDetails;
  readonly writeDetails?: WriteErrorDetails;
  private errorStatus: ErrorStatus;
  private readonly errorContext: Map<string, string>;

  constructor(
    kind: ErrorKind,
    message: string,
    options?: ScopeDBErrorOptions,
  ) {
    super(message, options?.cause === undefined ? undefined : { cause: options.cause });
    this.name = "ScopeDBError";
    this.kind = kind;
    this.code = options?.code;
    this.details = options?.details;
    this.httpStatus = options?.httpStatus;
    this.requestId = options?.requestId;
    this.retryAfterMs = options?.retryAfterMs;
    this.statementDetails = options?.statementDetails;
    this.writeDetails = options?.writeDetails;
    this.errorStatus = options?.status ?? "permanent";
    this.errorContext = new Map();
  }

  /** Whether retrying is appropriate for this error classification. */
  get retryable(): boolean {
    return this.errorStatus === "temporary";
  }

  withContext(key: string, value: string | number | boolean): this {
    this.errorContext.set(key, String(value));
    return this;
  }

  context(): ReadonlyMap<string, string> {
    return this.errorContext;
  }

  status(): ErrorStatus {
    return this.errorStatus;
  }

  setPermanent(): this {
    this.errorStatus = "permanent";
    return this;
  }

  setTemporary(): this {
    this.errorStatus = "temporary";
    return this;
  }

  setPersistent(): this {
    this.errorStatus = "persistent";
    return this;
  }

  isPermanent(): boolean {
    return this.errorStatus === "permanent";
  }

  isTemporary(): boolean {
    return this.errorStatus === "temporary";
  }

  isPersistent(): boolean {
    return this.errorStatus === "persistent";
  }

  override toString(): string {
    let s = `ScopeDBError [${this.kind}/${this.errorStatus}]: ${this.message}`;
    const metadata = new Map(this.errorContext);
    if (this.writeDetails !== undefined) {
      metadata.set("write_reason", this.writeDetails.reason);
      metadata.set("write_outcome", this.writeDetails.outcome);
    }
    if (this.httpStatus !== undefined) {
      metadata.set("http_status", String(this.httpStatus));
    }
    if (this.requestId !== undefined) {
      metadata.set("request_id", this.requestId);
    }
    if (this.retryAfterMs !== undefined) {
      metadata.set("retry_after_ms", String(this.retryAfterMs));
    }
    if (metadata.size > 0) {
      const ctx = [...metadata.entries()]
        .map(([k, v]) => `${k}=${v}`)
        .join(", ");
      s += ` { ${ctx} }`;
    }
    return s;
  }
}

export class AppendRowsError extends ScopeDBError {
  readonly appendState: AppendRowsErrorPayload["append_state"];
  readonly rowErrors: readonly AppendRowError[];
  readonly rowErrorsTruncated: boolean;

  constructor(
    payload: AppendRowsErrorPayload,
    message: string,
    options?: ScopeDBErrorOptions,
  ) {
    super("AppendRowsFailed", message, options);
    this.name = "AppendRowsError";
    this.appendState = payload.append_state;
    this.rowErrors = payload.row_errors;
    this.rowErrorsTruncated = payload.row_errors_truncated;
  }
}

export function asScopeDBError(
  kind: ErrorKind,
  message: string,
  cause: unknown,
): ScopeDBError {
  if (cause instanceof ScopeDBError) {
    return cause;
  }
  return new ScopeDBError(kind, message, { cause });
}

export function writeErrorDetailsFromCause(cause: unknown): WriteErrorDetails | undefined {
  const visited = new Set<unknown>();
  while (cause instanceof Error && !visited.has(cause)) {
    visited.add(cause);
    const code = "code" in cause ? cause.code : undefined;
    if (
      (cause instanceof DOMException && cause.name === "TimeoutError") ||
      code === "UND_ERR_CONNECT_TIMEOUT" ||
      code === "UND_ERR_HEADERS_TIMEOUT" ||
      code === "UND_ERR_BODY_TIMEOUT"
    ) {
      return { reason: "timeout", outcome: "unknown" };
    }
    cause = cause.cause;
  }
  return undefined;
}
