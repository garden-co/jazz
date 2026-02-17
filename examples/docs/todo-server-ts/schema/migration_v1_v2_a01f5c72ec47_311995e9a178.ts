import { migrate, col } from "jazz-tools";

migrate("todos", {
  description: col.add().string({ default: "" }),
});
