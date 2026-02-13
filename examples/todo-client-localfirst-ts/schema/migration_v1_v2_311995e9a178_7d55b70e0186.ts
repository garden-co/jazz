import { migrate, col } from "jazz-ts"

migrate("todos", {
  parent: col.add().optional().string({ default: null }),
})
