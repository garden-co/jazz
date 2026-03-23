// #region migration-generated-stub-example
import { migrate, col } from "jazz-tools";

migrate("todos", {
  description: col.add().optional().string({ default: null }),
});
// #endregion migration-generated-stub-example
