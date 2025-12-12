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
    default: () => {
      const owner = co.group().create();
      owner.addMember("everyone", "writer");
      return owner;
    },
  });
export type Issue = co.loaded<typeof Issue>;
