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

import type { RequestOptions } from "./client.js";
import type { DataType } from "./protocol.js";

export interface CatalogOptions extends RequestOptions {
  /** Total deadline for a catalog mutation, including its response body. */
  timeoutMs?: number;
  dryRun?: boolean;
  allowDestructive?: boolean;
  ifMatch?: string;
}

export interface CatalogDocument {
  databases?: CatalogDatabase[];
}
export interface CatalogDatabase {
  name: string;
  comment?: string | null;
  schemas?: CatalogSchema[];
}
export interface CatalogSchema {
  name: string;
  comment?: string | null;
  tables?: CatalogTable[];
  views?: CatalogView[];
}
export interface CatalogColumn {
  name: string;
  data_type: Exclude<DataType, "u_int">;
  expression?: string | null;
  comment?: string | null;
  indexes?: Array<{ type: "search"; analyzer?: "log" }>;
}
export interface CatalogTable {
  name: string;
  comment?: string | null;
  columns?: CatalogColumn[];
  partition_by?: string[];
  cluster_by?: string[];
  distinct?: { on?: string[]; by?: string[] };
  retention_days?: number | null;
  storage?: Record<string, unknown>;
}
export interface CatalogView {
  name: string;
  statement: string;
  comment?: string | null;
}
export interface CatalogSnapshot<T> {
  definition: T;
  etag: string;
}
export interface CatalogResult<T> {
  before: T | null;
  after: T | null;
  destructive: boolean;
  etag: string;
}

type Transport = (
  method: string,
  path: string[],
  definition: unknown,
  options: CatalogOptions,
) => Promise<{ body: Record<string, unknown>; etag: string }>;

/** A complete declarative resource. Writes are single attempts and never automatically retried. */
export class CatalogResource<T> {
  constructor(
    private readonly transport: Transport,
    private readonly path: string[],
  ) {}

  async get(options: RequestOptions = {}): Promise<CatalogSnapshot<T>> {
    const result = await this.transport("GET", this.path, undefined, options);
    return { definition: result.body as T, etag: result.etag };
  }

  /** Replaces the entire resource. Omitted fields and children are cleared; use dryRun to preview. */
  async apply(
    definition: T,
    options: CatalogOptions = {},
  ): Promise<CatalogResult<T>> {
    const result = await this.transport("PUT", this.path, definition, options);
    return { ...result.body, etag: result.etag } as unknown as CatalogResult<T>;
  }

  /** Removes a resource; potentially destructive changes require allowDestructive. */
  async delete(options: CatalogOptions = {}): Promise<CatalogResult<T>> {
    const result = await this.transport(
      "DELETE",
      this.path,
      undefined,
      options,
    );
    return { ...result.body, etag: result.etag } as unknown as CatalogResult<T>;
  }
}

/** Selects resources by unescaped names; reads return complete definitions without pagination. */
export class Catalog {
  private readonly root: CatalogResource<CatalogDocument>;
  constructor(private readonly transport: Transport) {
    this.root = new CatalogResource(transport, []);
  }
  get(options: RequestOptions = {}): Promise<CatalogSnapshot<CatalogDocument>> {
    return this.root.get(options);
  }
  apply(
    definition: CatalogDocument,
    options: CatalogOptions = {},
  ): Promise<CatalogResult<CatalogDocument>> {
    return this.root.apply(definition, options);
  }
  database(database: string): CatalogResource<CatalogDatabase> {
    return new CatalogResource(this.transport, ["databases", database]);
  }
  schema(database: string, schema: string): CatalogResource<CatalogSchema> {
    return new CatalogResource(this.transport, [
      "databases",
      database,
      "schemas",
      schema,
    ]);
  }
  table(
    database: string,
    schema: string,
    table: string,
  ): CatalogResource<CatalogTable> {
    return new CatalogResource(this.transport, [
      "databases",
      database,
      "schemas",
      schema,
      "tables",
      table,
    ]);
  }
  view(
    database: string,
    schema: string,
    view: string,
  ): CatalogResource<CatalogView> {
    return new CatalogResource(this.transport, [
      "databases",
      database,
      "schemas",
      schema,
      "views",
      view,
    ]);
  }
}
