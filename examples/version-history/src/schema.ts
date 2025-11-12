/**
 * Learn about schemas here:
 * https://jazz.tools/docs/react/schemas/covalues
 */

import { co, z } from "jazz-tools";

export const Issue = co
  .map({
    title: z.string(),
    description: co.plainText().withPermissions({
      onInlineCreate: "sameAsContainer",
    }),
    estimate: z.number(),
    status: z.literal(["backlog", "in progress", "done"]),
  })
  .resolved({ description: true })
  .withPermissions({
    onCreate(newGroup) {
      newGroup.addMember("everyone", "writer");
    },
  });
export type Issue = co.loaded<typeof Issue>;
