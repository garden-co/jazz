import { migrate, col } from "jazz-tools"

migrate("jams", {
  beat_count: col.add().int({ default: 16 }),
})
