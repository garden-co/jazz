import { resetDatabase } from "./db";

await resetDatabase();
console.log("database reset and migrated");
process.exit(0);
