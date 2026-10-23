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
export type { ClientOptions, RequestOptions, WriteOptions } from "./client.js";
export {
  AppendRetryExhaustedError,
  AppendStream,
  AppendStreamBuilder,
} from "./append-stream.js";
export type {
  AppendAdmissionResult,
  AppendBarrierResult,
  AppendBatchFailureEvent,
  AppendCircuitBreakerOptions,
  AppendCircuitState,
  AppendDeliveryReport,
  AppendFailurePolicy,
  AppendStreamOptions,
  AppendStreamState,
  AppendStreamStats,
  AppendWaitOptions,
} from "./append-stream.js";
export { AppendRowsError, ScopeDBError } from "./errors.js";
export type {
  ErrorKind,
  ErrorStatus,
  ScopeDBErrorOptions,
  WriteErrorDetails,
  WriteErrorReason,
} from "./errors.js";
export type {
  AppendRowError,
  AppendRowsErrorPayload,
  AppendRowsResult,
  AppendState,
  DataType,
  StatementErrorDetails,
  StatementEstimatedProgress,
  StatementProgress,
  StatementSnapshot,
  StatementSnapshotCancelled,
  StatementSnapshotFailed,
  StatementSnapshotFinished,
  StatementSnapshotPending,
  StatementSnapshotRunning,
} from "./protocol.js";
export { FieldSchema, ResultSet, Schema } from "./result.js";
export type { IntegerMode, ResultOptions, Value } from "./result.js";
export { Statement, StatementHandle } from "./statement.js";
export type { WaitOptions } from "./statement.js";
export { Table } from "./table.js";
export type { TableOptions } from "./table.js";

export { Catalog, CatalogResource } from "./catalog.js";
export type {
  CatalogColumn,
  CatalogDatabase,
  CatalogDocument,
  CatalogOptions,
  CatalogResult,
  CatalogSchema,
  CatalogSnapshot,
  CatalogTable,
  CatalogView,
} from "./catalog.js";
