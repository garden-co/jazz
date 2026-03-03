import { migrate, col } from "jazz-tools";

migrate("todos", {
  creationDate: col.add().int({ default: 0 }),
});
