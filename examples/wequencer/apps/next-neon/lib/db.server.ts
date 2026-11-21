import "server-only";
import pg from "pg";

// Neon URLs say sslmode=require; verify-full keeps the same checks without pg's warning.
export const DATABASE_URL = (process.env.DATABASE_URL ?? "").replace("sslmode=require", "sslmode=verify-full");

/** Wequencer's tables, for the write handlers and the profile bootstrap. */
export const pool = new pg.Pool({ connectionString: DATABASE_URL, max: 10 });
pool.on("error", (error) => console.error(error));
