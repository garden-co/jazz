import { migrate, col } from "jazz-tools"

migrate("jams", {
  bpm: col.add().int({ default: 95 }),
})
