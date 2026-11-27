import { resetDatabase } from "./db.js";

await resetDatabase();
console.log("database reset and migrated");
