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

// TODO(legacy): Remove this adapter and its call sites when dropping ScopeDB 0.5.
// This release retains the paginated catalog, rows, transform-ingest, and
// exec_timeout requests. ScopeDB 0.6 must provide those compatibility endpoints.
// Adapt statement envelopes here without selecting versions or retrying writes.

import { ScopeDBError } from "./errors.js";
import type { StatementCancelResult, StatementStatus } from "./protocol.js";
import { isStatementStatus } from "./protocol.js";

export function decodeStatement(value: unknown): StatementStatus {
  const status = withMessage(value);
  if (!isStatementStatus(status)) {
    throw new ScopeDBError("Unexpected", "statement response has an invalid body");
  }
  return status;
}

export function decodeCancellation(value: unknown): StatementCancelResult {
  let result = withMessage(value);
  if (isRecord(result) && result["status"] === "finished" && result["message"] === undefined) {
    result = { ...result, message: "statement is finished" };
  }
  if (
    !isStatementCancelResult(result) ||
    (("progress" in result || "error" in result || "result_set" in result) && !isStatementStatus(result))
  ) {
    throw new ScopeDBError("Unexpected", "statement cancellation response has an invalid body");
  }
  return result;
}

export function cancellationResult(status: StatementStatus): StatementCancelResult {
  if (status.status === "pending" || status.status === "running") {
    throw new ScopeDBError("Unexpected", "cannot summarize a nonterminal cancellation");
  }
  return {
    statement_id: status.statement_id,
    created_at: status.created_at,
    status: status.status,
    message: status.status === "finished" ? "statement is finished" : status.message,
  };
}

export function cancellationStatus(result: StatementCancelResult): StatementStatus | undefined {
  if (isStatementStatus(result)) {
    return result;
  }
  // A short 0.5 finished or failed response omits the result or failure details.
  // Leave it uncached so status()/wait() fetches the complete snapshot.
  if (result.status !== "cancelled") {
    return undefined;
  }
  return {
    ...result,
    status: "cancelled",
    progress: {
      total_percentage: 0,
      nanos_from_submitted: 0,
      nanos_from_started: 0,
      total_stages: 0,
      total_partitions: 0,
      total_rows: 0,
      total_compressed_bytes: 0,
      total_uncompressed_bytes: 0,
      scanned_stages: 0,
      scanned_partitions: 0,
      scanned_rows: 0,
      scanned_compressed_bytes: 0,
      scanned_uncompressed_bytes: 0,
      skipped_partitions: 0,
      skipped_rows: 0,
      skipped_compressed_bytes: 0,
      skipped_uncompressed_bytes: 0,
    },
  };
}

function withMessage(value: unknown): unknown {
  if (!isRecord(value) || value["message"] !== undefined) {
    return value;
  }
  if (value["status"] === "failed" && isRecord(value["error"])) {
    return { ...value, message: value["error"]["message"] };
  }
  if (value["status"] === "cancelled") {
    return { ...value, message: `statement is ${value["status"]}` };
  }
  return value;
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
