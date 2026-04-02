import { schema as s } from "jazz-tools";

void s;

// @ts-expect-error use `schema` as the single root schema export
import { col } from "jazz-tools";
// @ts-expect-error use `schema` as the single root schema export
import { table } from "jazz-tools";
// @ts-expect-error use `schema` as the single root schema export
import { migrate } from "jazz-tools";
// @ts-expect-error use `schema` as the single root schema export
import { defineSchema } from "jazz-tools";
// @ts-expect-error use `schema` as the single root schema export
import { defineApp } from "jazz-tools";
// @ts-expect-error use `schema` as the single root schema export
import { defineMigration } from "jazz-tools";
// @ts-expect-error use `schema` as the single root schema export
import { definePermissions } from "jazz-tools";

// @ts-expect-error use `s.Schema`
import type { Schema } from "jazz-tools";
// @ts-expect-error use `s.App`
import type { App } from "jazz-tools";
// @ts-expect-error use `s.RowOf`
import type { RowOf } from "jazz-tools";
// @ts-expect-error use `s.InsertOf`
import type { InsertOf } from "jazz-tools";
// @ts-expect-error use `s.WhereOf`
import type { WhereOf } from "jazz-tools";
// @ts-expect-error use `s.TableDefinition`
import type { TableDefinition } from "jazz-tools";
// @ts-expect-error use `s.SchemaDefinition`
import type { SchemaDefinition } from "jazz-tools";
// @ts-expect-error use `s.TableIndex`
import type { TableIndex } from "jazz-tools";
// @ts-expect-error use `s.TableMetaOf`
import type { TableMetaOf } from "jazz-tools";
