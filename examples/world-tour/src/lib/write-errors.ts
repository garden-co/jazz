import { ref } from "vue";

/**
 * The latest write the app couldn't save, shown as a toast by App.vue.
 *
 * Every rejection is reported exactly once, by whoever owns it:
 * - a write the app waits on (`write.wait(...)`) reports its own rejection from
 *   that wait, with a message that fits it. Jazz hands a rejection to the
 *   write's active wait, and the wait claims it;
 * - every other write is reported by App.vue's `db.onMutationError` listener,
 *   which Jazz only calls for rejections no wait has claimed.
 * So the same write never reaches both, and the toast never flips between two
 * messages for one rejection.
 */
export const writeError = ref("");

export function reportWriteError(
  error: unknown,
  message = "Couldn't save that change. It may have been undone.",
) {
  console.error("Write failed", error);
  writeError.value = message;
}
