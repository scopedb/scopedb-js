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
  | "u_int" // backward-compat alias for "uint"
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

interface StatementStatusBase {
  statement_id: string;
  created_at: string;
  progress: StatementEstimatedProgress;
}

export interface StatementStatusPending extends StatementStatusBase {
  status: "pending";
}

export interface StatementStatusRunning extends StatementStatusBase {
  status: "running";
}

export interface StatementStatusFinished extends StatementStatusBase {
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

export interface StatementStatusFailed extends StatementStatusBase {
  status: "failed";
  message: string;
  /** Structured server failure, absent when talking to an older server. */
  error?: StatementErrorDetails;
}

export interface StatementStatusCancelled extends StatementStatusBase {
  status: "cancelled";
  message: string;
}

export type StatementStatus =
  | StatementStatusPending
  | StatementStatusRunning
  | StatementStatusFinished
  | StatementStatusFailed
  | StatementStatusCancelled;

export interface StatementRequest {
  statement: string;
  statement_id?: string;
  exec_timeout?: string;
  format: ResultFormat;
}

export interface StatementCancelResult {
  statement_id: string;
  status: "finished" | "failed" | "cancelled";
  message: string;
  created_at: string;
}

export type IngestType = "committed" | "buffered";

export interface IngestRequest {
  type: IngestType;
  data: {
    format: "json";
    rows: string;
  };
  statement: string;
}

export interface IngestResult {
  num_rows_inserted: number;
}

/** The commit outcome used by table append responses and client-side write interruptions. */
export type AppendState = "committed" | "rejected" | "unknown";

export interface AppendRowsResult {
  append_state: "committed";
  num_rows_inserted: number;
}

export interface AppendRowError {
  row_index: number;
  column: string;
  message: string;
}

export interface AppendRowsErrorPayload {
  message: string;
  append_state: Exclude<AppendState, "committed">;
  row_errors: AppendRowError[];
  row_errors_truncated: boolean;
}

export interface CatalogPage<T> {
  items: T[];
  next_page_token?: string;
}

export interface DatabaseResource {
  name: string;
  comment: string | null;
}

export interface SchemaResource {
  database: string;
  name: string;
  comment: string | null;
}

export interface TableResourceSummary {
  database: string;
  schema: string;
  name: string;
  comment: string | null;
}

export interface TableColumnSpec {
  name: string;
  data_type: Exclude<DataType, "u_int">;
  comment: string | null;
}

export interface TableDistinctSpec {
  on: string[];
  by: string[];
}

export interface TableSpec {
  columns: TableColumnSpec[];
  partition_by: string[];
  cluster_by: string[];
  distinct_on: TableDistinctSpec;
  data_retention_days: number | null;
  comment: string | null;
}

export interface TableResource extends TableResourceSummary, TableSpec {}

export interface ErrorPayload {
  message: string;
}

export function statementIsFinished(status: StatementStatus): status is StatementStatusFinished {
  return status.status === "finished";
}

export function statementIsTerminated(status: StatementStatus): boolean {
  return (
    status.status === "finished" ||
    status.status === "failed" ||
    status.status === "cancelled"
  );
}

export function isStatementStatus(value: unknown): value is StatementStatus {
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

export function isDataType(value: unknown): value is DataType {
  return typeof value === "string" && DATA_TYPES.has(value as DataType);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
