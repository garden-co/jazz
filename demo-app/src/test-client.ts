/**
 * Test script for the Groove database client
 *
 * Run with: npx tsx src/test-client.ts
 */

import { createDatabase, previewQuery } from "./client.js";

// Create database client
const db = createDatabase();

console.log("=== Query Preview Examples (Prisma-style filters) ===\n");

// Simple query - all notes
console.log("1. All notes:");
console.log(previewQuery("Note"));
console.log();

// Query with simple equality (shorthand)
console.log("2. Notes with title = 'Hello' (shorthand):");
console.log(previewQuery("Note", { where: { title: "Hello" } }));
console.log();

// Query with equals filter
console.log("3. Notes with title equals 'Hello' (explicit):");
console.log(previewQuery("Note", { where: { title: { equals: "Hello" } } }));
console.log();

// Query with contains filter (string)
console.log("4. Notes with title containing 'meeting':");
console.log(previewQuery("Note", { where: { title: { contains: "meeting" } } }));
console.log();

// Query with startsWith filter
console.log("5. Notes with title starting with 'TODO':");
console.log(previewQuery("Note", { where: { title: { startsWith: "TODO" } } }));
console.log();

// Query with numeric comparison (bigint)
console.log("6. Notes created after timestamp:");
console.log(previewQuery("Note", { where: { createdAt: { gte: 1700000000000n } } }));
console.log();

// Query with OR combinator
console.log("7. Notes with title 'A' OR title 'B':");
console.log(previewQuery("Note", { where: { OR: [{ title: "A" }, { title: "B" }] } }));
console.log();

// Query with AND combinator
console.log("8. Notes with title containing 'meeting' AND created after timestamp:");
console.log(previewQuery("Note", {
  where: {
    AND: [
      { title: { contains: "meeting" } },
      { createdAt: { gte: 1700000000000n } }
    ]
  }
}));
console.log();

// Query with NOT combinator
console.log("9. Notes NOT with title 'Draft':");
console.log(previewQuery("Note", { where: { NOT: { title: "Draft" } } }));
console.log();

// Query with null check
console.log("10. Notes without a folder (folder is null):");
console.log(previewQuery("Note", { where: { folder: null } }));
console.log();

// Query with not null check
console.log("11. Notes with a folder (folder is not null):");
console.log(previewQuery("Note", { where: { folder: { not: null } } }));
console.log();

// Query with include (forward ref)
console.log("12. Notes with author loaded:");
console.log(previewQuery("Note", { include: { author: true } }));
console.log();

// Query with include (reverse ref / array subquery)
console.log("13. Users with their notes:");
console.log(previewQuery("User", { include: { Notes: true } }));
console.log();

// Complex query with where + include
console.log("14. Notes containing 'important' with author and folder:");
console.log(previewQuery("Note", {
  where: { title: { contains: "important" } },
  include: { author: true, folder: true }
}));
console.log();

console.log("=== Subscribe Examples ===\n");

// Test subscribe with typed where
console.log("15. db.note.subscribeAll with type-safe where:");
const unsub = db.note.subscribeAll(
  {
    where: {
      OR: [
        { title: { contains: "meeting" } },
        { createdAt: { gte: BigInt(Date.now()) } }
      ]
    },
    include: { author: true },
  },
  (notes) => {
    // TypeScript knows: notes is NoteLoaded<{ author: true }>[]
    // So notes[0].author would be User (not ObjectId)
    console.log("Received notes:", notes);
  }
);
console.log();

// Cleanup
unsub();

console.log("=== Type Safety Demo ===\n");

// These would cause TypeScript errors if uncommented:
// db.note.subscribeAll({ where: { invalidColumn: "x" } }, () => {}); // Error: 'invalidColumn' does not exist
// db.note.subscribeAll({ where: { title: 123 } }, () => {}); // Error: number not assignable to string | StringFilter
// db.note.subscribeAll({ where: { createdAt: "not a bigint" } }, () => {}); // Error: string not assignable to bigint | BigIntFilter

console.log("TypeScript provides autocomplete for:");
console.log("- Column names (title, content, author, folder, createdAt, updatedAt)");
console.log("- Filter operators (equals, not, contains, startsWith, endsWith, gt, gte, lt, lte, in, notIn)");
console.log("- Combinators (AND, OR, NOT)");
