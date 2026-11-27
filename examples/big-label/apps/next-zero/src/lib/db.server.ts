import pg from "pg";
import { zeroNodePg } from "@rocicorp/zero/server/adapters/pg";
import { schema } from "../../schema";

export const DATABASE_URL =
  process.env.ZERO_UPSTREAM_DB ?? "postgres://postgres:pass@127.0.0.1:5432/big_label_zero";

/** The app's tables, for Zero's mutate endpoint and the trusted server routes. */
export const pool = new pg.Pool({ connectionString: DATABASE_URL });
export const dbProvider = zeroNodePg(schema, pool);
