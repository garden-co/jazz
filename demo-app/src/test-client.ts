/**
 * Test script for the Groove database client
 *
 * Run with: npx tsx src/test-client.ts
 */

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import init, { WasmDatabase, initSync } from "../pkg/groove_wasm.js";
import { createDatabase, previewQuery } from "./client.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

async function main() {
  // Initialize WASM with inline bytes (for Node.js)
  const wasmPath = join(__dirname, "../pkg/groove_wasm_bg.wasm");
  const wasmBytes = readFileSync(wasmPath);
  initSync(wasmBytes);

  console.log("=== Groove Database Client Test ===\n");

  // Create WASM database
  const wasmDb = new WasmDatabase();

  // Create typed client
  const db = createDatabase(wasmDb);

  // Create schema
  console.log("Creating schema...");
  db.raw.execute(`
    CREATE TABLE User (
      name STRING NOT NULL,
      email STRING NOT NULL,
      avatar STRING
    )
  `);
  db.raw.execute(`
    CREATE TABLE Folder (
      name STRING NOT NULL,
      owner REFERENCES User NOT NULL,
      parent REFERENCES Folder
    )
  `);
  db.raw.execute(`
    CREATE TABLE Note (
      title STRING NOT NULL,
      content STRING NOT NULL,
      author REFERENCES User NOT NULL,
      folder REFERENCES Folder,
      createdAt I64 NOT NULL,
      updatedAt I64 NOT NULL
    )
  `);
  db.raw.execute(`
    CREATE TABLE Tag (
      name STRING NOT NULL,
      color STRING NOT NULL
    )
  `);
  console.log("Schema created.\n");

  // Track callback invocations
  let userCallbackCount = 0;
  let noteCallbackCount = 0;

  // Test 1: Subscribe to all users
  console.log("Test 1: subscribeAll users");
  const unsubUsers = db.user.subscribeAll({}, (users) => {
    userCallbackCount++;
    console.log(`  [Callback #${userCallbackCount}] Users:`, users.map(u => u.name));
  });

  // Insert some users
  console.log("\nInserting users...");
  const user1Result = db.raw.execute(`INSERT INTO User (name, email) VALUES ('Alice', 'alice@example.com')`);
  console.log("  Inserted user 1:", user1Result);

  const user2Result = db.raw.execute(`INSERT INTO User (name, email) VALUES ('Bob', 'bob@example.com')`);
  console.log("  Inserted user 2:", user2Result);

  // Test 2: Subscribe to all notes
  console.log("\nTest 2: subscribeAll notes");
  const unsubNotes = db.note.subscribeAll({}, (notes) => {
    noteCallbackCount++;
    console.log(`  [Callback #${noteCallbackCount}] Notes:`, notes.map(n => n.title));
  });

  // Get user IDs from results
  const user1Id = String(user1Result).replace("inserted:", "");
  const user2Id = String(user2Result).replace("inserted:", "");
  console.log(`  User IDs: ${user1Id}, ${user2Id}`);

  // Insert some notes
  console.log("\nInserting notes...");
  const now = BigInt(Date.now());
  const note1Result = db.raw.execute(`
    INSERT INTO Note (title, content, author, createdAt, updatedAt)
    VALUES ('First Note', 'Hello world!', '${user1Id}', ${now}, ${now})
  `);
  console.log("  Inserted note 1:", note1Result);

  const note2Result = db.raw.execute(`
    INSERT INTO Note (title, content, author, createdAt, updatedAt)
    VALUES ('Second Note', 'Another note.', '${user2Id}', ${now}, ${now})
  `);
  console.log("  Inserted note 2:", note2Result);

  // Test 3: Subscribe with where clause
  console.log("\nTest 3: subscribeAll notes with where clause (author filter)");
  let filteredNoteCount = 0;
  const unsubFiltered = db.note.subscribeAll(
    { where: { author: user1Id } },
    (notes) => {
      filteredNoteCount++;
      console.log(`  [Filtered callback #${filteredNoteCount}] Alice's notes:`, notes.map(n => n.title));
    }
  );

  // Insert another note for Alice
  const note3Result = db.raw.execute(`
    INSERT INTO Note (title, content, author, createdAt, updatedAt)
    VALUES ('Third Note', 'Alice writes again.', '${user1Id}', ${now}, ${now})
  `);
  console.log("  Inserted note 3:", note3Result);

  // Test 4: Update a note
  console.log("\nTest 4: Update a note");
  const note2Id = String(note2Result).replace("inserted:", "");
  db.raw.update_row("Note", note2Id, "title", "Updated Second Note");
  console.log("  Updated note 2 title");

  // Cleanup
  console.log("\nCleaning up subscriptions...");
  unsubUsers();
  unsubNotes();
  unsubFiltered();

  console.log("\n=== Summary ===");
  console.log(`User callback invocations: ${userCallbackCount}`);
  console.log(`Note callback invocations: ${noteCallbackCount}`);
  console.log(`Filtered note callback invocations: ${filteredNoteCount}`);

  console.log("\n=== Query Preview Examples ===\n");

  // Preview some queries
  console.log("All notes:");
  console.log("  ", previewQuery("Note"));

  console.log("\nNotes with author included:");
  console.log("  ", previewQuery("Note", { include: { author: true } }));

  console.log("\nUsers with their notes (reverse ref):");
  console.log("  ", previewQuery("User", { include: { Note: true } }));

  console.log("\nNotes with title filter:");
  console.log("  ", previewQuery("Note", { where: { title: { contains: "important" } } }));

  console.log("\n=== Done ===");
}

main().catch(console.error);
