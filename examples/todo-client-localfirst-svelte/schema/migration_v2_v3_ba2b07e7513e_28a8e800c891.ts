import { migrate, col } from "jazz-tools";

migrate("todos", {
  owner_id: col.add().string({ default: "" }),
});
