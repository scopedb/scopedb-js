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

export type DataType =
  | "int"
  | "uint"
  | "float"
  | "timestamp"
  | "interval"
  | "boolean"
  | "string"
  | "binary"
  | "array"
  | "object"
  | "any"
  | "null";

export interface FieldSchemaPayload {
  name: string;
  data_type: DataType;
}

export interface ResultSetMetadata {
  fields: FieldSchemaPayload[];
  num_rows: number;
}

export type ResultFormat = "json";

export interface StatementResultSet {
  metadata: ResultSetMetadata;
  format: ResultFormat;
  rows: Array<Array<string | null>>;
}

export interface StatementProgress {
  compute_seconds: number;
  total_stages: number;
  total_partitions: number;
  total_rows: number;
  total_compressed_bytes: number;
  total_uncompressed_bytes: number;
  scanned_stages: number;
  scanned_partitions: number;
  scanned_rows: number;
  scanned_compressed_bytes: number;
  scanned_uncompressed_bytes: number;
  skipped_partitions: number;
  skipped_rows: number;
  skipped_compressed_bytes: number;
  skipped_uncompressed_bytes: number;
}

export interface StatementEstimatedProgress extends StatementProgress {
  total_percentage: number;
  nanos_from_submitted: number;
  nanos_from_started: number;
}

interface StatementSnapshotBase {
  statement_id: string;
  created_at: string;
  progress: StatementEstimatedProgress;
}

export interface StatementSnapshotPending extends StatementSnapshotBase {
  status: "pending";
}

export interface StatementSnapshotRunning extends StatementSnapshotBase {
  status: "running";
}

export interface StatementSnapshotFinished extends StatementSnapshotBase {
  status: "finished";
  result_set: StatementResultSet;
}

export interface StatementErrorDetails {
  /** Stable machine-readable failure code. Unknown codes are preserved. */
  code: string;
  message: string;
  /** Code-specific details returned by ScopeDB. */
  details?: unknown;
}

export interface StatementSnapshotFailed extends StatementSnapshotBase {
  status: "failed";
  error: StatementErrorDetails;
}

export interface StatementSnapshotCancelled extends StatementSnapshotBase {
  status: "cancelled";
}

export type StatementSnapshot =
  | StatementSnapshotPending
  | StatementSnapshotRunning
  | StatementSnapshotFinished
  | StatementSnapshotFailed
  | StatementSnapshotCancelled;

export interface StatementRequest {
  statement: string;
  statement_id?: string;
  max_elapsed_seconds?: number;
  max_estimated_seconds?: number;
  format: ResultFormat;
}

export type AppendState = "committed" | "rejected" | "unknown";

export interface AppendRowsResult {
  status: "committed";
  num_rows_inserted: number;
}

export interface AppendRowError {
  row_index: number;
  column: string;
  message: string;
}

/** Normalized SDK diagnostics; Client decodes the server's nested details.status. */
export interface AppendRowsErrorPayload {
  message: string;
  status: Exclude<AppendState, "committed">;
  row_errors: AppendRowError[];
  row_errors_truncated: boolean;
}

export interface ErrorPayload {
  code: string;
  message: string;
  details?: unknown;
}

export function statementIsFinished(
  status: StatementSnapshot,
): status is StatementSnapshotFinished {
  return status.status === "finished";
}

export function statementIsTerminated(status: StatementSnapshot): boolean {
  return (
    status.status === "finished" ||
    status.status === "failed" ||
    status.status === "cancelled"
  );
}

const DATA_TYPES = new Set<DataType>([
  "int",
  "uint",
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

export function isDataType(value: unknown): value is DataType {
  return typeof value === "string" && DATA_TYPES.has(value as DataType);
}
