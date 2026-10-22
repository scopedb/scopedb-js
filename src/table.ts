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

import type { Client, RequestOptions, WriteOptions } from "./client.js";
import {
  AppendStreamBuilder,
  type AppendFailurePolicy,
  type AppendStreamOptions,
} from "./append-stream.js";
import type { AppendRowsResult } from "./protocol.js";
import type { CatalogTable, CatalogOptions, CatalogResult } from "./catalog.js";
import { FieldSchema, Schema } from "./result.js";

export interface TableOptions {
  database?: string;
  schema?: string;
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
    await this.client
      .statement(`DROP TABLE ${this.identifier()}`)
      .execute(options);
  }

  /** Appends at most 8 MiB of uncompressed newline-delimited JSON. */
  async append(
    ndjson: string,
    options: WriteOptions = {},
  ): Promise<AppendRowsResult> {
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
    const failurePolicy =
      options === undefined ? "stop" : options?.failurePolicy;
    return AppendStreamBuilder.create(
      this.client,
      this.databaseName ?? "scopedb",
      this.schemaName ?? "public",
      this.tableName,
      failurePolicy as AppendFailurePolicy,
    );
  }

  /** Returns the table's declarative definition. */
  async describe(options: RequestOptions = {}): Promise<CatalogTable> {
    return (await this.resource().get(options)).definition;
  }

  /** Replaces the complete definition; potentially destructive changes require explicit authorization. */
  apply(
    definition: CatalogTable,
    options: CatalogOptions = {},
  ): Promise<CatalogResult<CatalogTable>> {
    return this.resource().apply(definition, options);
  }

  /** @deprecated Use `describe()`. */
  async tableSchema(options: RequestOptions = {}): Promise<Schema> {
    const table = await this.describe(options);
    return new Schema(
      (table.columns ?? []).map(
        (column) => new FieldSchema(column.name, column.data_type),
      ),
    );
  }

  private resource() {
    return this.client
      .catalog()
      .table(
        this.databaseName ?? "scopedb",
        this.schemaName ?? "public",
        this.tableName,
      );
  }
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
