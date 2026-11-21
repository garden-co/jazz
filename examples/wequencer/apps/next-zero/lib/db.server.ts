import pg from "pg";

export const DATABASE_URL =
  process.env.ZERO_UPSTREAM_DB ?? "postgres://postgres:pass@127.0.0.1:5432/wequencer_zero";

/** Wequencer's tables (public schema), for Zero's mutate endpoint and the profile bootstrap. */
export const pool = new pg.Pool({ connectionString: DATABASE_URL });
