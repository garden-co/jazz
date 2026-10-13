import { ref } from "vue";

/**
 * The latest write the app couldn't save, shown as a toast by App.vue. Rejections
 * the server sends later arrive through `db.onMutationError`; local failures of
 * fire-and-forget writes are passed in with `reportWriteError`.
 */
export const writeError = ref("");

export function reportWriteError(error: unknown) {
  console.error("Write failed", error);
  writeError.value = "Couldn't save that change. It may have been undone.";
}
