import { migrate, col } from "jazz-ts";

migrate("todos", {
  description: col.add().string({ default: "" }),
});
