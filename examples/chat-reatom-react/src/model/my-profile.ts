import { computed, effect, withConnectHook, wrap } from "@reatom/core";
import { jazz } from "@/jazz";
import { getRandomUsername } from "@/lib/utils";
import { app, type Profile } from "../../schema.js";

const myProfileQuery = jazz.reatomQueryAll(() => {
  const userId = jazz().session?.user_id ?? "__none__";
  return app.profiles.where({ userId });
}, "myProfileQuery");

const createdForUser = new Set<string>();

export const resetProfileGuard = () => createdForUser.clear();

export const myProfile = computed((): Profile | null => {
  const profiles = myProfileQuery();
  const sorted = [...profiles].sort((a, b) => a.id.localeCompare(b.id));
  return (sorted[0] as Profile | undefined) ?? null;
}, "myProfile").extend(
  withConnectHook(() => {
    effect(() => {
      const userId = jazz().session?.user_id;
      if (!userId) return;
      const profiles = myProfileQuery();
      if (profiles.length > 0 || createdForUser.has(userId)) return;
      createdForUser.add(userId);
      const { db } = jazz();
      const opts = db.getConfig().serverUrl ? { tier: "edge" as const } : undefined;
      const handle = db.insert(app.profiles, {
        userId,
        name: getRandomUsername(),
      });
      if (opts) {
        wrap(handle.wait(opts)).catch(() => createdForUser.delete(userId));
      }
    }, "myProfile.autocreate");
  }),
);
