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
  AppendRowsErrorPayload,
  StatementErrorDetails,
} from "./protocol.js";

export type ErrorKind =
  | "Unexpected"
  | "ConfigInvalid"
  /** The statement execution was rejected by the server (failed or cancelled in-band). */
  | "StatementFailed"
  /** The table append was rejected or its commit outcome is unknown. */
  | "AppendRowsFailed";
export type ErrorStatus = "permanent" | "temporary" | "persistent";

export interface ScopeDBErrorOptions {
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
}

export class ScopeDBError extends Error {
  readonly kind: ErrorKind;
  readonly httpStatus?: number;
  readonly requestId?: string;
  readonly retryAfterMs?: number;
  readonly statementDetails?: StatementErrorDetails;
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
    this.httpStatus = options?.httpStatus;
    this.requestId = options?.requestId;
    this.retryAfterMs = options?.retryAfterMs;
    this.statementDetails = options?.statementDetails;
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
