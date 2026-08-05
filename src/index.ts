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

export { Client } from "./client.js";
export type { CatalogListOptions, ClientOptions, RequestOptions } from "./client.js";
export { AppendStream, AppendStreamBuilder } from "./append-stream.js";
export type {
  AppendAdmissionResult,
  AppendBarrierResult,
  AppendBatchFailureEvent,
  AppendCircuitBreakerOptions,
  AppendCircuitState,
  AppendDeliveryReport,
  AppendFailurePolicy,
  AnyAppendStream,
  AppendStreamOptions,
  AppendStreamState,
  AppendStreamStats,
  AppendWaitOptions,
} from "./append-stream.js";
export { AppendRowsError, ScopeDBError } from "./errors.js";
export type { ErrorKind, ErrorStatus } from "./errors.js";
export { IngestStream, IngestStreamBuilder } from "./ingest-stream.js";
export type {
  AppendRowError,
  AppendRowsErrorPayload,
  AppendRowsResult,
  AppendState,
  CatalogPage,
  DataType,
  DatabaseResource,
  IngestResult,
  SchemaResource,
  StatementCancelResult,
  StatementEstimatedProgress,
  StatementProgress,
  StatementStatus,
  StatementStatusCancelled,
  StatementStatusFailed,
  StatementStatusFinished,
  StatementStatusPending,
  StatementStatusRunning,
  TableColumnSpec,
  TableDistinctSpec,
  TableResource,
  TableResourceSummary,
  TableSpec,
} from "./protocol.js";
export { FieldSchema, ResultSet, Schema } from "./result.js";
export type { IntegerMode, IntoOptions, Value } from "./result.js";
export { Statement, StatementHandle } from "./statement.js";
export type { FetchOptions } from "./statement.js";
export { Table } from "./table.js";
