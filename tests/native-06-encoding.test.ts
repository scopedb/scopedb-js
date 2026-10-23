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

import assert from "node:assert/strict";
import { test } from "node:test";
import { AppendRowsError, Client, ScopeDBError } from "../src/index.js";

test("native 0.6 Append JSON encodings and atomic rejection", {
  skip: !process.env["SCOPEDB_ENDPOINT"],
}, async () => {
  const endpoint = process.env["SCOPEDB_ENDPOINT"]!;
  const apiKey = process.env["SCOPEDB_API_KEY"];
  const client = new Client(endpoint, { apiKey });
  const schemaName = `sdk06_js_encoding_${Date.now()}`;
  const schema = client.catalog().schema("scopedb", schemaName);
  const table = client.table("encodings", { schema: schemaName });
  try {
    await schema.apply({
      name: schemaName,
      tables: [{
        name: "encodings",
        columns: [
          { name: "id", data_type: "int" },
          { name: "unsigned", data_type: "uint" },
          { name: "score", data_type: "float" },
          { name: "ts", data_type: "timestamp" },
          { name: "attrs", data_type: "object" },
          { name: "items", data_type: "array" },
          { name: "value", data_type: "any" },
          { name: "payload", data_type: "binary" },
          { name: "elapsed", data_type: "interval" },
          { name: "enabled", data_type: "boolean" },
          { name: "label", data_type: "string" },
          { name: "empty", data_type: "string" },
          { name: "doubled", data_type: "int", expression: "id * 2" },
        ],
      }],
    });

    const valid = [
      '{"id":1,"unsigned":9223372036854775807,"score":1.5,"ts":"2026-10-23T12:34:56.123456789Z","attrs":{"nested":null,"ok":true},"items":[1,"two",null],"value":{"a":[true]},"payload":"aGVsbG8=","elapsed":"PT1H2M3.000000004S","enabled":true,"label":"hello","empty":null}',
      '{"id":2,"score":1e3,"ts":"2026-10-23T20:34:56+08:00","value":[1,"two"],"elapsed":"-PT0.5S"}',
      '{"id":3,"score":9007199254740992,"value":true}',
      '{"id":4,"value":42}',
      '{"id":5,"value":"2026-10-23T12:34:56Z"}',
      '{"id":6,"value":null}',
      "{}",
    ];
    assert.equal(
      (await table.append(valid.join("\n"))).num_rows_inserted,
      valid.length,
    );
    const result = await client.query(
      `FROM ${table.identifier()} WHERE id = 1 SELECT unsigned, score, ts, attrs["ok"]::boolean AS ok, items[0]::int AS first_item, enabled, label, empty, doubled`,
    );
    const row = result.first()!;
    assert.equal(row["unsigned"], 9223372036854775807n);
    assert.equal(row["score"], 1.5);
    assert.equal(row["ok"], true);
    assert.equal(row["first_item"], 1n);
    assert.equal(row["enabled"], true);
    assert.equal(row["label"], "hello");
    assert.equal(row["empty"], null);
    assert.equal(row["doubled"], 2n);
    assert.equal(result.rawRows()[0]?.[2], "2026-10-23T12:34:56.123456789Z");
    assert.equal(
      (await client.query(
        `FROM ${table.identifier()} WHERE id = 2 SELECT enabled`,
      )).first()?.["enabled"],
      null,
    );
    for (
      const input of [
        '{"id":"42"}',
        '{"id":42.0}',
        '{"id":42e0}',
        '{"unsigned":-1}',
        '{"unsigned":9223372036854775808}',
        '{"score":9007199254740993}',
        '{"score":1e999}',
        '{"score":1e-999}',
        '{"score":NaN}',
        '{"score":"NaN"}',
        '{"score":Infinity}',
        '{"ts":"2026-10-23"}',
        '{"ts":"2026-10-23T12:34:56"}',
        '{"ts":1792758896}',
        '{"attrs":"{\\"ok\\":true}"}',
        '{"items":{}}',
        '{"payload":"aGVsbG8"}',
        '{"payload":"_w=="}',
        '{"elapsed":"P1D"}',
        '{"enabled":1}',
        '{"label":1}',
        '{"extra":null}',
        '{"doubled":null}',
        '{"attrs":{"a":1,"a":2}}',
        '{"value":{"id":9223372036854775808}}',
      ]
    ) {
      // A valid row preceding the error must not be committed from the same request.
      await assert.rejects(table.append(`{"id":999}\n${input}`), (error) => {
        assert.ok(error instanceof AppendRowsError, input);
        assert.equal(error.outcome, "rejected", input);
        assert.ok([400, 422].includes(error.httpStatus ?? 0), input);
        return true;
      });
    }
    assert.equal(
      (await client.query(`FROM ${table.identifier()} AGGREGATE count() AS n`))
        .first()?.["n"],
      BigInt(valid.length),
    );
    console.log(
      "ENCODINGS native types, missing/null columns, computed fields, and atomic rejection passed",
    );
  } finally {
    try {
      await schema.delete({ allowDestructive: true });
    } catch (error) {
      if (!(error instanceof ScopeDBError) || error.httpStatus !== 404) {
        throw error;
      }
    }
    console.log("CLEANUP encoding schema removed");
  }
});
