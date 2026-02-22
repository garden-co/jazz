import { migrate, col } from "jazz-tools";

// Example of editing a generated migration stub.
migrate("todos", {
  description: col.add().string({ default: "No description" }),
});
