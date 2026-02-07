import { co, z } from "jazz-tools";

const Point = z.object({
  x: z.number(),
  y: z.number(),
});
export type Point = z.infer<typeof Point>;

const Stroke = z.object({
  id: z.string(),
  points: z.array(Point),
  color: z.string(),
  width: z.number(),
  createdAt: z.number(),
});
export type Stroke = z.infer<typeof Stroke>;

export const Canvas = co.record(z.string(), z.array(Stroke)).withPermissions({
  default: () => {
    const defaultGroup = co.group().create();
    defaultGroup.addMember("everyone", "writer");
    return defaultGroup;
  },
});

export const ImageAttachment = co
  .map({
    type: z.literal("image"),
    name: z.string(),
    attachment: co.image(),
  })
  .withPermissions({
    default: () => {
      const defaultGroup = co.group().create();
      defaultGroup.addMember("everyone", "writer");
      return defaultGroup;
    },
  });
export const FileAttachment = co
  .map({
    type: z.literal("file"),
    name: z.string(),
    attachment: co.fileStream(),
  })
  .withPermissions({
    default: () => {
      const defaultGroup = co.group().create();
      defaultGroup.addMember("everyone", "writer");
      return defaultGroup;
    },
  });

export const CanvasAttachment = co
  .map({
    type: z.literal("canvas"),
    name: z.string(),
    canvas: Canvas,
  })
  .withPermissions({
    default: () => {
      const defaultGroup = co.group().create();
      defaultGroup.addMember("everyone", "writer");
      return defaultGroup;
    },
  });

export const Attachment = co.discriminatedUnion("type", [
  ImageAttachment,
  FileAttachment,
  CanvasAttachment,
]);

export const Message = co
  .map({
    text: co.plainText(),
    attachment: co.optional(Attachment),
    reactions: co.feed(z.string()),
  })
  .resolved({
    text: true,
    attachment: true,
    reactions: true,
  })
  .withPermissions({
    onInlineCreate: "sameAsContainer",
  });

export const Chat = co.list(Message).withPermissions({
  default: () => {
    const defaultGroup = co.group().create();
    defaultGroup.addMember("everyone", "writer");
    return defaultGroup;
  },
});

export const ChatProfile = co.profile({
  name: z.string(),
  avatar: co.image().optional(),
});

export const ChatAccount = co
  .account({
    profile: ChatProfile,
    root: co.map({
      chats: co.record(z.string(), Chat),
    }),
  })
  .withMigration(async (account) => {
    if (!account.$jazz.has("root")) {
      account.$jazz.set("root", {
        chats: {},
      });
    }
  });

export const ChatAccountWithProfile = ChatAccount.resolved({
  profile: true,
});
