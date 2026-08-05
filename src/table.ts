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

import type { Client, RequestOptions } from "./client.js";
import {
  AppendStreamBuilder,
  type AppendFailurePolicy,
  type AppendStreamOptions,
} from "./append-stream.js";
import type {
  AppendRowsResult,
  DataType,
  TableResource,
} from "./protocol.js";
import { FieldSchema, Schema } from "./result.js";

export interface TableOptions {
  database?: string;
  schema?: string;
}

export interface TableColumn {
  name: string;
  dataType: Exclude<DataType, "u_int">;
  comment: string | null;
}

export interface TableDescription {
  database: string;
  schema: string;
  name: string;
  columns: TableColumn[];
  partitionBy: string[];
  clusterBy: string[];
  distinctOn: {
    on: string[];
    by: string[];
  };
  dataRetentionDays: number | null;
  comment: string | null;
}

export class Table {
  private databaseName?: string;
  private schemaName?: string;

  constructor(
    private readonly client: Client,
    private readonly tableName: string,
    options: TableOptions = {},
  ) {
    this.databaseName = options.database;
    this.schemaName = options.schema;
  }

  /** @deprecated Pass `database` to `Client.table()` instead. */
  withDatabase(database: string): this {
    this.databaseName = database;
    return this;
  }

  /** @deprecated Pass `schema` to `Client.table()` instead. */
  withSchema(schema: string): this {
    this.schemaName = schema;
    return this;
  }

  identifier(): string {
    const parts: string[] = [];
    if (this.databaseName !== undefined) {
      parts.push(quoteIdent(this.databaseName, "`"));
    }
    if (this.schemaName !== undefined) {
      parts.push(quoteIdent(this.schemaName, "`"));
    }
    parts.push(quoteIdent(this.tableName, "`"));
    return parts.join(".");
  }

  async drop(options: RequestOptions = {}): Promise<void> {
    await this.client.statement(`DROP TABLE ${this.identifier()}`).execute(options);
  }

  /** Appends newline-delimited JSON rows to this table. */
  async append(ndjson: string, options: RequestOptions = {}): Promise<AppendRowsResult> {
    return this.client.appendRows(
      this.databaseName ?? "scopedb",
      this.schemaName ?? "public",
      this.tableName,
      ndjson,
      options,
    );
  }

  /** Builds an asynchronous, concurrent NDJSON append stream for this table. */
  appendStream(): AppendStreamBuilder<"stop">;
  appendStream(options: undefined): AppendStreamBuilder<"stop">;
  /** Builds an append stream with an explicit batch-failure policy. */
  appendStream<Policy extends AppendFailurePolicy>(
    options: AppendStreamOptions<Policy>,
  ): AppendStreamBuilder<Policy>;
  appendStream<Policy extends AppendFailurePolicy>(
    options: AppendStreamOptions<Policy> | undefined,
  ): AppendStreamBuilder<Policy | "stop">;
  appendStream(
    options?: AppendStreamOptions<AppendFailurePolicy>,
  ): AppendStreamBuilder<AppendFailurePolicy> {
    const failurePolicy = options === undefined
      ? "stop"
      : options?.failurePolicy;
    return AppendStreamBuilder.create(
      this.client,
      this.databaseName ?? "scopedb",
      this.schemaName ?? "public",
      this.tableName,
      failurePolicy as AppendFailurePolicy,
    );
  }

  /** Returns this table's complete catalog metadata with JS-style field names. */
  async describe(options: RequestOptions = {}): Promise<TableDescription> {
    const table = await this.fetchResource(options);
    return tableDescription(table);
  }

  /** @deprecated Use `describe()` when possible. */
  async tableSchema(options: RequestOptions = {}): Promise<Schema> {
    const table = await this.fetchResource(options);

    return new Schema(
      table.columns.map((column) => new FieldSchema(column.name, column.data_type)),
    );
  }

  private fetchResource(options: RequestOptions): Promise<TableResource> {
    return this.client.fetchTable({
      ...options,
      database: this.databaseName ?? "scopedb",
      schema: this.schemaName ?? "public",
      table: this.tableName,
    });
  }
}

function tableDescription(table: TableResource): TableDescription {
  return {
    database: table.database,
    schema: table.schema,
    name: table.name,
    columns: table.columns.map((column) => ({
      name: column.name,
      dataType: column.data_type,
      comment: column.comment,
    })),
    partitionBy: [...table.partition_by],
    clusterBy: [...table.cluster_by],
    distinctOn: {
      on: [...table.distinct_on.on],
      by: [...table.distinct_on.by],
    },
    dataRetentionDays: table.data_retention_days,
    comment: table.comment,
  };
}

function quoteIdent(input: string, quote: string): string {
  return quoteScopeQL(input, quote);
}

function quoteScopeQL(input: string, quote: string): string {
  let out = quote;
  for (const ch of input) {
    switch (ch) {
      case "\t":
        out += "\\t";
        break;
      case "\n":
        out += "\\n";
        break;
      case "\r":
        out += "\\r";
        break;
      case "\\":
        out += "\\\\";
        break;
      default:
        if (ch === quote) {
          out += `\\${ch}`;
        } else if (ch < " ") {
          out += `\\x${ch.charCodeAt(0).toString(16).padStart(2, "0")}`;
        } else {
          out += ch;
        }
    }
  }
  out += quote;
  return out;
}
