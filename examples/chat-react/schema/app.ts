// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | { [key: string]: JsonValue }
  | JsonValue[];

export interface Profile {
  id: string;
  userId: string;
  name: string;
  avatar?: string;
}

export interface Chat {
  id: string;
  isPublic: boolean;
  createdBy: string;
}

export interface ChatMember {
  id: string;
  chat: string;
  userId: string;
  joinCode?: string;
}

export interface Message {
  id: string;
  chat: string;
  text: string;
  sender: string;
  senderId: string;
  createdAt: Date;
}

export interface Reaction {
  id: string;
  message: string;
  userId: string;
  emoji: string;
}

export interface Canvas {
  id: string;
  chat: string;
  createdAt: Date;
}

export interface Stroke {
  id: string;
  canvas: string;
  ownerId: string;
  color: string;
  width: number;
  pointsJson: string;
  createdAt: Date;
}

export interface Attachment {
  id: string;
  message: string;
  type: string;
  name: string;
  data: string;
  mimeType: string;
  size: number;
}

export interface ProfileInit {
  userId: string;
  name: string;
  avatar?: string;
}

export interface ChatInit {
  isPublic: boolean;
  createdBy: string;
}

export interface ChatMemberInit {
  chat: string;
  userId: string;
  joinCode?: string;
}

export interface MessageInit {
  chat: string;
  text: string;
  sender: string;
  senderId: string;
  createdAt: Date;
}

export interface ReactionInit {
  message: string;
  userId: string;
  emoji: string;
}

export interface CanvasInit {
  chat: string;
  createdAt: Date;
}

export interface StrokeInit {
  canvas: string;
  ownerId: string;
  color: string;
  width: number;
  pointsJson: string;
  createdAt: Date;
}

export interface AttachmentInit {
  message: string;
  type: string;
  name: string;
  data: string;
  mimeType: string;
  size: number;
}

export interface ProfileWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  userId?: string | { eq?: string; ne?: string; contains?: string };
  name?: string | { eq?: string; ne?: string; contains?: string };
  avatar?: string | { eq?: string; ne?: string; contains?: string };
}

export interface ChatWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  isPublic?: boolean;
  createdBy?: string | { eq?: string; ne?: string; contains?: string };
}

export interface ChatMemberWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  chat?: string | { eq?: string; ne?: string };
  userId?: string | { eq?: string; ne?: string; contains?: string };
  joinCode?: string | { eq?: string; ne?: string; contains?: string };
}

export interface MessageWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  chat?: string | { eq?: string; ne?: string };
  text?: string | { eq?: string; ne?: string; contains?: string };
  sender?: string | { eq?: string; ne?: string };
  senderId?: string | { eq?: string; ne?: string; contains?: string };
  createdAt?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
}

export interface ReactionWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  message?: string | { eq?: string; ne?: string };
  userId?: string | { eq?: string; ne?: string; contains?: string };
  emoji?: string | { eq?: string; ne?: string; contains?: string };
}

export interface CanvasWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  chat?: string | { eq?: string; ne?: string };
  createdAt?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
}

export interface StrokeWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  canvas?: string | { eq?: string; ne?: string };
  ownerId?: string | { eq?: string; ne?: string; contains?: string };
  color?: string | { eq?: string; ne?: string; contains?: string };
  width?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  pointsJson?: string | { eq?: string; ne?: string; contains?: string };
  createdAt?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
}

export interface AttachmentWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  message?: string | { eq?: string; ne?: string };
  type?: string | { eq?: string; ne?: string; contains?: string };
  name?: string | { eq?: string; ne?: string; contains?: string };
  data?: string | { eq?: string; ne?: string; contains?: string };
  mimeType?: string | { eq?: string; ne?: string; contains?: string };
  size?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
}

export interface ProfileInclude {
  messagesViaSender?: true | MessageInclude | MessageQueryBuilder;
}

export interface ChatInclude {
  chatMembersViaChat?: true | ChatMemberInclude | ChatMemberQueryBuilder;
  messagesViaChat?: true | MessageInclude | MessageQueryBuilder;
  canvasesViaChat?: true | CanvasInclude | CanvasQueryBuilder;
}

export interface ChatMemberInclude {
  chat?: true | ChatInclude | ChatQueryBuilder;
}

export interface MessageInclude {
  chat?: true | ChatInclude | ChatQueryBuilder;
  sender?: true | ProfileInclude | ProfileQueryBuilder;
  reactionsViaMessage?: true | ReactionInclude | ReactionQueryBuilder;
  attachmentsViaMessage?: true | AttachmentInclude | AttachmentQueryBuilder;
}

export interface ReactionInclude {
  message?: true | MessageInclude | MessageQueryBuilder;
}

export interface CanvasInclude {
  chat?: true | ChatInclude | ChatQueryBuilder;
  strokesViaCanvas?: true | StrokeInclude | StrokeQueryBuilder;
}

export interface StrokeInclude {
  canvas?: true | CanvasInclude | CanvasQueryBuilder;
}

export interface AttachmentInclude {
  message?: true | MessageInclude | MessageQueryBuilder;
}

export interface ProfileRelations {
  messagesViaSender: Message[];
}

export interface ChatRelations {
  chatMembersViaChat: ChatMember[];
  messagesViaChat: Message[];
  canvasesViaChat: Canvas[];
}

export interface ChatMemberRelations {
  chat: Chat;
}

export interface MessageRelations {
  chat: Chat;
  sender: Profile;
  reactionsViaMessage: Reaction[];
  attachmentsViaMessage: Attachment[];
}

export interface ReactionRelations {
  message: Message;
}

export interface CanvasRelations {
  chat: Chat;
  strokesViaCanvas: Stroke[];
}

export interface StrokeRelations {
  canvas: Canvas;
}

export interface AttachmentRelations {
  message: Message;
}

export type ProfileWithIncludes<I extends ProfileInclude = {}> = Profile & {
  messagesViaSender?: NonNullable<I["messagesViaSender"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Message[]
      : RelationInclude extends MessageQueryBuilder<
            infer QueryInclude extends MessageInclude,
            infer QuerySelect extends keyof Message | "*"
          >
        ? MessageSelectedWithIncludes<QueryInclude, QuerySelect>[]
        : RelationInclude extends MessageInclude
          ? MessageWithIncludes<RelationInclude>[]
          : never
    : never;
};

export type ChatWithIncludes<I extends ChatInclude = {}> = Chat & {
  chatMembersViaChat?: NonNullable<I["chatMembersViaChat"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? ChatMember[]
      : RelationInclude extends ChatMemberQueryBuilder<
            infer QueryInclude extends ChatMemberInclude,
            infer QuerySelect extends keyof ChatMember | "*"
          >
        ? ChatMemberSelectedWithIncludes<QueryInclude, QuerySelect>[]
        : RelationInclude extends ChatMemberInclude
          ? ChatMemberWithIncludes<RelationInclude>[]
          : never
    : never;
  messagesViaChat?: NonNullable<I["messagesViaChat"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Message[]
      : RelationInclude extends MessageQueryBuilder<
            infer QueryInclude extends MessageInclude,
            infer QuerySelect extends keyof Message | "*"
          >
        ? MessageSelectedWithIncludes<QueryInclude, QuerySelect>[]
        : RelationInclude extends MessageInclude
          ? MessageWithIncludes<RelationInclude>[]
          : never
    : never;
  canvasesViaChat?: NonNullable<I["canvasesViaChat"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Canvas[]
      : RelationInclude extends CanvasQueryBuilder<
            infer QueryInclude extends CanvasInclude,
            infer QuerySelect extends keyof Canvas | "*"
          >
        ? CanvasSelectedWithIncludes<QueryInclude, QuerySelect>[]
        : RelationInclude extends CanvasInclude
          ? CanvasWithIncludes<RelationInclude>[]
          : never
    : never;
};

export type ChatMemberWithIncludes<I extends ChatMemberInclude = {}> = ChatMember & {
  chat?: NonNullable<I["chat"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Chat
      : RelationInclude extends ChatQueryBuilder<
            infer QueryInclude extends ChatInclude,
            infer QuerySelect extends keyof Chat | "*"
          >
        ? ChatSelectedWithIncludes<QueryInclude, QuerySelect>
        : RelationInclude extends ChatInclude
          ? ChatWithIncludes<RelationInclude>
          : never
    : never;
};

export type MessageWithIncludes<I extends MessageInclude = {}> = Message & {
  chat?: NonNullable<I["chat"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Chat
      : RelationInclude extends ChatQueryBuilder<
            infer QueryInclude extends ChatInclude,
            infer QuerySelect extends keyof Chat | "*"
          >
        ? ChatSelectedWithIncludes<QueryInclude, QuerySelect>
        : RelationInclude extends ChatInclude
          ? ChatWithIncludes<RelationInclude>
          : never
    : never;
  sender?: NonNullable<I["sender"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Profile
      : RelationInclude extends ProfileQueryBuilder<
            infer QueryInclude extends ProfileInclude,
            infer QuerySelect extends keyof Profile | "*"
          >
        ? ProfileSelectedWithIncludes<QueryInclude, QuerySelect>
        : RelationInclude extends ProfileInclude
          ? ProfileWithIncludes<RelationInclude>
          : never
    : never;
  reactionsViaMessage?: NonNullable<I["reactionsViaMessage"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Reaction[]
      : RelationInclude extends ReactionQueryBuilder<
            infer QueryInclude extends ReactionInclude,
            infer QuerySelect extends keyof Reaction | "*"
          >
        ? ReactionSelectedWithIncludes<QueryInclude, QuerySelect>[]
        : RelationInclude extends ReactionInclude
          ? ReactionWithIncludes<RelationInclude>[]
          : never
    : never;
  attachmentsViaMessage?: NonNullable<I["attachmentsViaMessage"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Attachment[]
      : RelationInclude extends AttachmentQueryBuilder<
            infer QueryInclude extends AttachmentInclude,
            infer QuerySelect extends keyof Attachment | "*"
          >
        ? AttachmentSelectedWithIncludes<QueryInclude, QuerySelect>[]
        : RelationInclude extends AttachmentInclude
          ? AttachmentWithIncludes<RelationInclude>[]
          : never
    : never;
};

export type ReactionWithIncludes<I extends ReactionInclude = {}> = Reaction & {
  message?: NonNullable<I["message"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Message
      : RelationInclude extends MessageQueryBuilder<
            infer QueryInclude extends MessageInclude,
            infer QuerySelect extends keyof Message | "*"
          >
        ? MessageSelectedWithIncludes<QueryInclude, QuerySelect>
        : RelationInclude extends MessageInclude
          ? MessageWithIncludes<RelationInclude>
          : never
    : never;
};

export type CanvasWithIncludes<I extends CanvasInclude = {}> = Canvas & {
  chat?: NonNullable<I["chat"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Chat
      : RelationInclude extends ChatQueryBuilder<
            infer QueryInclude extends ChatInclude,
            infer QuerySelect extends keyof Chat | "*"
          >
        ? ChatSelectedWithIncludes<QueryInclude, QuerySelect>
        : RelationInclude extends ChatInclude
          ? ChatWithIncludes<RelationInclude>
          : never
    : never;
  strokesViaCanvas?: NonNullable<I["strokesViaCanvas"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Stroke[]
      : RelationInclude extends StrokeQueryBuilder<
            infer QueryInclude extends StrokeInclude,
            infer QuerySelect extends keyof Stroke | "*"
          >
        ? StrokeSelectedWithIncludes<QueryInclude, QuerySelect>[]
        : RelationInclude extends StrokeInclude
          ? StrokeWithIncludes<RelationInclude>[]
          : never
    : never;
};

export type StrokeWithIncludes<I extends StrokeInclude = {}> = Stroke & {
  canvas?: NonNullable<I["canvas"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Canvas
      : RelationInclude extends CanvasQueryBuilder<
            infer QueryInclude extends CanvasInclude,
            infer QuerySelect extends keyof Canvas | "*"
          >
        ? CanvasSelectedWithIncludes<QueryInclude, QuerySelect>
        : RelationInclude extends CanvasInclude
          ? CanvasWithIncludes<RelationInclude>
          : never
    : never;
};

export type AttachmentWithIncludes<I extends AttachmentInclude = {}> = Attachment & {
  message?: NonNullable<I["message"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Message
      : RelationInclude extends MessageQueryBuilder<
            infer QueryInclude extends MessageInclude,
            infer QuerySelect extends keyof Message | "*"
          >
        ? MessageSelectedWithIncludes<QueryInclude, QuerySelect>
        : RelationInclude extends MessageInclude
          ? MessageWithIncludes<RelationInclude>
          : never
    : never;
};

export type ProfileSelected<S extends keyof Profile | "*" = keyof Profile> = "*" extends S
  ? Profile
  : Pick<Profile, Extract<S | "id", keyof Profile>>;

export type ProfileSelectedWithIncludes<
  I extends ProfileInclude = {},
  S extends keyof Profile | "*" = keyof Profile,
> = ProfileSelected<S> & Omit<ProfileWithIncludes<I>, keyof Profile>;

export type ChatSelected<S extends keyof Chat | "*" = keyof Chat> = "*" extends S
  ? Chat
  : Pick<Chat, Extract<S | "id", keyof Chat>>;

export type ChatSelectedWithIncludes<
  I extends ChatInclude = {},
  S extends keyof Chat | "*" = keyof Chat,
> = ChatSelected<S> & Omit<ChatWithIncludes<I>, keyof Chat>;

export type ChatMemberSelected<S extends keyof ChatMember | "*" = keyof ChatMember> = "*" extends S
  ? ChatMember
  : Pick<ChatMember, Extract<S | "id", keyof ChatMember>>;

export type ChatMemberSelectedWithIncludes<
  I extends ChatMemberInclude = {},
  S extends keyof ChatMember | "*" = keyof ChatMember,
> = ChatMemberSelected<S> & Omit<ChatMemberWithIncludes<I>, keyof ChatMember>;

export type MessageSelected<S extends keyof Message | "*" = keyof Message> = "*" extends S
  ? Message
  : Pick<Message, Extract<S | "id", keyof Message>>;

export type MessageSelectedWithIncludes<
  I extends MessageInclude = {},
  S extends keyof Message | "*" = keyof Message,
> = MessageSelected<S> & Omit<MessageWithIncludes<I>, keyof Message>;

export type ReactionSelected<S extends keyof Reaction | "*" = keyof Reaction> = "*" extends S
  ? Reaction
  : Pick<Reaction, Extract<S | "id", keyof Reaction>>;

export type ReactionSelectedWithIncludes<
  I extends ReactionInclude = {},
  S extends keyof Reaction | "*" = keyof Reaction,
> = ReactionSelected<S> & Omit<ReactionWithIncludes<I>, keyof Reaction>;

export type CanvasSelected<S extends keyof Canvas | "*" = keyof Canvas> = "*" extends S
  ? Canvas
  : Pick<Canvas, Extract<S | "id", keyof Canvas>>;

export type CanvasSelectedWithIncludes<
  I extends CanvasInclude = {},
  S extends keyof Canvas | "*" = keyof Canvas,
> = CanvasSelected<S> & Omit<CanvasWithIncludes<I>, keyof Canvas>;

export type StrokeSelected<S extends keyof Stroke | "*" = keyof Stroke> = "*" extends S
  ? Stroke
  : Pick<Stroke, Extract<S | "id", keyof Stroke>>;

export type StrokeSelectedWithIncludes<
  I extends StrokeInclude = {},
  S extends keyof Stroke | "*" = keyof Stroke,
> = StrokeSelected<S> & Omit<StrokeWithIncludes<I>, keyof Stroke>;

export type AttachmentSelected<S extends keyof Attachment | "*" = keyof Attachment> = "*" extends S
  ? Attachment
  : Pick<Attachment, Extract<S | "id", keyof Attachment>>;

export type AttachmentSelectedWithIncludes<
  I extends AttachmentInclude = {},
  S extends keyof Attachment | "*" = keyof Attachment,
> = AttachmentSelected<S> & Omit<AttachmentWithIncludes<I>, keyof Attachment>;

export const wasmSchema: WasmSchema = {
  profiles: {
    columns: [
      {
        name: "userId",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "name",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "avatar",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
    ],
    policies: {
      select: {
        using: {
          type: "True",
        },
      },
      insert: {
        with_check: {
          type: "Cmp",
          column: "userId",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
      },
      update: {
        using: {
          type: "Cmp",
          column: "userId",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
        with_check: {
          type: "Cmp",
          column: "userId",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
      },
      delete: {},
    },
  },
  chats: {
    columns: [
      {
        name: "isPublic",
        column_type: {
          type: "Boolean",
        },
        nullable: false,
      },
      {
        name: "createdBy",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
    ],
    policies: {
      select: {
        using: {
          type: "Or",
          exprs: [
            {
              type: "Cmp",
              column: "isPublic",
              op: "Eq",
              value: {
                type: "Literal",
                value: {
                  type: "Boolean",
                  value: true,
                },
              },
            },
            {
              type: "Exists",
              table: "chatMembers",
              condition: {
                type: "And",
                exprs: [
                  {
                    type: "Cmp",
                    column: "chat",
                    op: "Eq",
                    value: {
                      type: "SessionRef",
                      path: ["__jazz_outer_row", "id"],
                    },
                  },
                  {
                    type: "Cmp",
                    column: "userId",
                    op: "Eq",
                    value: {
                      type: "SessionRef",
                      path: ["user_id"],
                    },
                  },
                ],
              },
            },
            {
              type: "Exists",
              table: "chatMembers",
              condition: {
                type: "And",
                exprs: [
                  {
                    type: "Cmp",
                    column: "chat",
                    op: "Eq",
                    value: {
                      type: "SessionRef",
                      path: ["__jazz_outer_row", "id"],
                    },
                  },
                  {
                    type: "Cmp",
                    column: "joinCode",
                    op: "Eq",
                    value: {
                      type: "SessionRef",
                      path: ["claims", "join_code"],
                    },
                  },
                ],
              },
            },
          ],
        },
      },
      insert: {
        with_check: {
          type: "Cmp",
          column: "createdBy",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
      },
      update: {},
      delete: {},
    },
  },
  chatMembers: {
    columns: [
      {
        name: "chat",
        column_type: {
          type: "Uuid",
        },
        nullable: false,
        references: "chats",
      },
      {
        name: "userId",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "joinCode",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
    ],
    policies: {
      select: {
        using: {
          type: "Cmp",
          column: "userId",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
      },
      insert: {
        with_check: {
          type: "Cmp",
          column: "userId",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
      },
      update: {},
      delete: {},
    },
  },
  messages: {
    columns: [
      {
        name: "chat",
        column_type: {
          type: "Uuid",
        },
        nullable: false,
        references: "chats",
      },
      {
        name: "text",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "sender",
        column_type: {
          type: "Uuid",
        },
        nullable: false,
        references: "profiles",
      },
      {
        name: "senderId",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "createdAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
    ],
    policies: {
      select: {
        using: {
          type: "Or",
          exprs: [
            {
              type: "Inherits",
              operation: "Select",
              via_column: "chat",
            },
            {
              type: "Exists",
              table: "chatMembers",
              condition: {
                type: "And",
                exprs: [
                  {
                    type: "Cmp",
                    column: "chat",
                    op: "Eq",
                    value: {
                      type: "SessionRef",
                      path: ["__jazz_outer_row", "chat"],
                    },
                  },
                  {
                    type: "Cmp",
                    column: "userId",
                    op: "Eq",
                    value: {
                      type: "SessionRef",
                      path: ["user_id"],
                    },
                  },
                ],
              },
            },
          ],
        },
      },
      insert: {
        with_check: {
          type: "Exists",
          table: "chatMembers",
          condition: {
            type: "And",
            exprs: [
              {
                type: "Cmp",
                column: "chat",
                op: "Eq",
                value: {
                  type: "SessionRef",
                  path: ["__jazz_outer_row", "chat"],
                },
              },
              {
                type: "Cmp",
                column: "userId",
                op: "Eq",
                value: {
                  type: "SessionRef",
                  path: ["user_id"],
                },
              },
            ],
          },
        },
      },
      update: {},
      delete: {
        using: {
          type: "Cmp",
          column: "senderId",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
      },
    },
  },
  reactions: {
    columns: [
      {
        name: "message",
        column_type: {
          type: "Uuid",
        },
        nullable: false,
        references: "messages",
      },
      {
        name: "userId",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "emoji",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
    ],
    policies: {
      select: {
        using: {
          type: "Inherits",
          operation: "Select",
          via_column: "message",
        },
      },
      insert: {
        with_check: {
          type: "Cmp",
          column: "userId",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
      },
      update: {},
      delete: {
        using: {
          type: "Cmp",
          column: "userId",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
      },
    },
  },
  canvases: {
    columns: [
      {
        name: "chat",
        column_type: {
          type: "Uuid",
        },
        nullable: false,
        references: "chats",
      },
      {
        name: "createdAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
    ],
    policies: {
      select: {
        using: {
          type: "Or",
          exprs: [
            {
              type: "Inherits",
              operation: "Select",
              via_column: "chat",
            },
            {
              type: "Exists",
              table: "chatMembers",
              condition: {
                type: "And",
                exprs: [
                  {
                    type: "Cmp",
                    column: "chat",
                    op: "Eq",
                    value: {
                      type: "SessionRef",
                      path: ["__jazz_outer_row", "chat"],
                    },
                  },
                  {
                    type: "Cmp",
                    column: "userId",
                    op: "Eq",
                    value: {
                      type: "SessionRef",
                      path: ["user_id"],
                    },
                  },
                ],
              },
            },
          ],
        },
      },
      insert: {
        with_check: {
          type: "Exists",
          table: "chatMembers",
          condition: {
            type: "And",
            exprs: [
              {
                type: "Cmp",
                column: "chat",
                op: "Eq",
                value: {
                  type: "SessionRef",
                  path: ["__jazz_outer_row", "chat"],
                },
              },
              {
                type: "Cmp",
                column: "userId",
                op: "Eq",
                value: {
                  type: "SessionRef",
                  path: ["user_id"],
                },
              },
            ],
          },
        },
      },
      update: {},
      delete: {},
    },
  },
  strokes: {
    columns: [
      {
        name: "canvas",
        column_type: {
          type: "Uuid",
        },
        nullable: false,
        references: "canvases",
      },
      {
        name: "ownerId",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "color",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "width",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
      {
        name: "pointsJson",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "createdAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
    ],
    policies: {
      select: {
        using: {
          type: "Inherits",
          operation: "Select",
          via_column: "canvas",
        },
      },
      insert: {
        with_check: {
          type: "Inherits",
          operation: "Select",
          via_column: "canvas",
        },
      },
      update: {},
      delete: {
        using: {
          type: "Cmp",
          column: "ownerId",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
      },
    },
  },
  attachments: {
    columns: [
      {
        name: "message",
        column_type: {
          type: "Uuid",
        },
        nullable: false,
        references: "messages",
      },
      {
        name: "type",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "name",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "data",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "mimeType",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "size",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
    ],
    policies: {
      select: {
        using: {
          type: "Inherits",
          operation: "Select",
          via_column: "message",
        },
      },
      insert: {
        with_check: {
          type: "Inherits",
          operation: "Select",
          via_column: "message",
        },
      },
      update: {},
      delete: {},
    },
  },
};

export class ProfileQueryBuilder<
  I extends ProfileInclude = {},
  S extends keyof Profile | "*" = keyof Profile,
> implements QueryBuilder<ProfileSelectedWithIncludes<I, S>> {
  readonly _table = "profiles";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: ProfileSelectedWithIncludes<I, S>;
  declare readonly _initType: ProfileInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<ProfileInclude> = {};
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: ProfileWhereInput): ProfileQueryBuilder<I, S> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends keyof Profile | "*">(
    ...columns: [NewS, ...NewS[]]
  ): ProfileQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ProfileInclude>(relations: NewI): ProfileQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: keyof Profile, direction: "asc" | "desc" = "asc"): ProfileQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ProfileQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ProfileQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "messagesViaSender"): ProfileQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: ProfileWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): ProfileQueryBuilder<I, S> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0];
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends ProfileInclude = I,
    CloneS extends keyof Profile | "*" = S,
  >(): ProfileQueryBuilder<CloneI, CloneS> {
    const clone = new ProfileQueryBuilder<CloneI, CloneS>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class ChatQueryBuilder<
  I extends ChatInclude = {},
  S extends keyof Chat | "*" = keyof Chat,
> implements QueryBuilder<ChatSelectedWithIncludes<I, S>> {
  readonly _table = "chats";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: ChatSelectedWithIncludes<I, S>;
  declare readonly _initType: ChatInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<ChatInclude> = {};
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: ChatWhereInput): ChatQueryBuilder<I, S> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends keyof Chat | "*">(...columns: [NewS, ...NewS[]]): ChatQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ChatInclude>(relations: NewI): ChatQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: keyof Chat, direction: "asc" | "desc" = "asc"): ChatQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ChatQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ChatQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(
    relation: "chatMembersViaChat" | "messagesViaChat" | "canvasesViaChat",
  ): ChatQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: ChatWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): ChatQueryBuilder<I, S> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0];
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends ChatInclude = I,
    CloneS extends keyof Chat | "*" = S,
  >(): ChatQueryBuilder<CloneI, CloneS> {
    const clone = new ChatQueryBuilder<CloneI, CloneS>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class ChatMemberQueryBuilder<
  I extends ChatMemberInclude = {},
  S extends keyof ChatMember | "*" = keyof ChatMember,
> implements QueryBuilder<ChatMemberSelectedWithIncludes<I, S>> {
  readonly _table = "chatMembers";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: ChatMemberSelectedWithIncludes<I, S>;
  declare readonly _initType: ChatMemberInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<ChatMemberInclude> = {};
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: ChatMemberWhereInput): ChatMemberQueryBuilder<I, S> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends keyof ChatMember | "*">(
    ...columns: [NewS, ...NewS[]]
  ): ChatMemberQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ChatMemberInclude>(relations: NewI): ChatMemberQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(
    column: keyof ChatMember,
    direction: "asc" | "desc" = "asc",
  ): ChatMemberQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ChatMemberQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ChatMemberQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "chat"): ChatMemberQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: ChatMemberWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): ChatMemberQueryBuilder<I, S> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0];
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends ChatMemberInclude = I,
    CloneS extends keyof ChatMember | "*" = S,
  >(): ChatMemberQueryBuilder<CloneI, CloneS> {
    const clone = new ChatMemberQueryBuilder<CloneI, CloneS>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class MessageQueryBuilder<
  I extends MessageInclude = {},
  S extends keyof Message | "*" = keyof Message,
> implements QueryBuilder<MessageSelectedWithIncludes<I, S>> {
  readonly _table = "messages";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: MessageSelectedWithIncludes<I, S>;
  declare readonly _initType: MessageInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<MessageInclude> = {};
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: MessageWhereInput): MessageQueryBuilder<I, S> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends keyof Message | "*">(
    ...columns: [NewS, ...NewS[]]
  ): MessageQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends MessageInclude>(relations: NewI): MessageQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: keyof Message, direction: "asc" | "desc" = "asc"): MessageQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): MessageQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): MessageQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(
    relation: "chat" | "sender" | "reactionsViaMessage" | "attachmentsViaMessage",
  ): MessageQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: MessageWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): MessageQueryBuilder<I, S> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0];
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends MessageInclude = I,
    CloneS extends keyof Message | "*" = S,
  >(): MessageQueryBuilder<CloneI, CloneS> {
    const clone = new MessageQueryBuilder<CloneI, CloneS>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class ReactionQueryBuilder<
  I extends ReactionInclude = {},
  S extends keyof Reaction | "*" = keyof Reaction,
> implements QueryBuilder<ReactionSelectedWithIncludes<I, S>> {
  readonly _table = "reactions";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: ReactionSelectedWithIncludes<I, S>;
  declare readonly _initType: ReactionInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<ReactionInclude> = {};
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: ReactionWhereInput): ReactionQueryBuilder<I, S> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends keyof Reaction | "*">(
    ...columns: [NewS, ...NewS[]]
  ): ReactionQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ReactionInclude>(relations: NewI): ReactionQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: keyof Reaction, direction: "asc" | "desc" = "asc"): ReactionQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ReactionQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ReactionQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "message"): ReactionQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: ReactionWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): ReactionQueryBuilder<I, S> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0];
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends ReactionInclude = I,
    CloneS extends keyof Reaction | "*" = S,
  >(): ReactionQueryBuilder<CloneI, CloneS> {
    const clone = new ReactionQueryBuilder<CloneI, CloneS>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class CanvasQueryBuilder<
  I extends CanvasInclude = {},
  S extends keyof Canvas | "*" = keyof Canvas,
> implements QueryBuilder<CanvasSelectedWithIncludes<I, S>> {
  readonly _table = "canvases";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: CanvasSelectedWithIncludes<I, S>;
  declare readonly _initType: CanvasInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<CanvasInclude> = {};
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: CanvasWhereInput): CanvasQueryBuilder<I, S> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends keyof Canvas | "*">(
    ...columns: [NewS, ...NewS[]]
  ): CanvasQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends CanvasInclude>(relations: NewI): CanvasQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: keyof Canvas, direction: "asc" | "desc" = "asc"): CanvasQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): CanvasQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): CanvasQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "chat" | "strokesViaCanvas"): CanvasQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: CanvasWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): CanvasQueryBuilder<I, S> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0];
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends CanvasInclude = I,
    CloneS extends keyof Canvas | "*" = S,
  >(): CanvasQueryBuilder<CloneI, CloneS> {
    const clone = new CanvasQueryBuilder<CloneI, CloneS>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class StrokeQueryBuilder<
  I extends StrokeInclude = {},
  S extends keyof Stroke | "*" = keyof Stroke,
> implements QueryBuilder<StrokeSelectedWithIncludes<I, S>> {
  readonly _table = "strokes";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: StrokeSelectedWithIncludes<I, S>;
  declare readonly _initType: StrokeInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<StrokeInclude> = {};
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: StrokeWhereInput): StrokeQueryBuilder<I, S> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends keyof Stroke | "*">(
    ...columns: [NewS, ...NewS[]]
  ): StrokeQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends StrokeInclude>(relations: NewI): StrokeQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: keyof Stroke, direction: "asc" | "desc" = "asc"): StrokeQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): StrokeQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): StrokeQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "canvas"): StrokeQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: StrokeWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): StrokeQueryBuilder<I, S> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0];
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends StrokeInclude = I,
    CloneS extends keyof Stroke | "*" = S,
  >(): StrokeQueryBuilder<CloneI, CloneS> {
    const clone = new StrokeQueryBuilder<CloneI, CloneS>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class AttachmentQueryBuilder<
  I extends AttachmentInclude = {},
  S extends keyof Attachment | "*" = keyof Attachment,
> implements QueryBuilder<AttachmentSelectedWithIncludes<I, S>> {
  readonly _table = "attachments";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: AttachmentSelectedWithIncludes<I, S>;
  declare readonly _initType: AttachmentInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<AttachmentInclude> = {};
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: AttachmentWhereInput): AttachmentQueryBuilder<I, S> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends keyof Attachment | "*">(
    ...columns: [NewS, ...NewS[]]
  ): AttachmentQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends AttachmentInclude>(relations: NewI): AttachmentQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(
    column: keyof Attachment,
    direction: "asc" | "desc" = "asc",
  ): AttachmentQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): AttachmentQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): AttachmentQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "message"): AttachmentQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: AttachmentWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): AttachmentQueryBuilder<I, S> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0];
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends AttachmentInclude = I,
    CloneS extends keyof Attachment | "*" = S,
  >(): AttachmentQueryBuilder<CloneI, CloneS> {
    const clone = new AttachmentQueryBuilder<CloneI, CloneS>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export interface GeneratedApp {
  profiles: ProfileQueryBuilder;
  chats: ChatQueryBuilder;
  chatMembers: ChatMemberQueryBuilder;
  messages: MessageQueryBuilder;
  reactions: ReactionQueryBuilder;
  canvases: CanvasQueryBuilder;
  strokes: StrokeQueryBuilder;
  attachments: AttachmentQueryBuilder;
  wasmSchema: WasmSchema;
}

export const app: GeneratedApp = {
  profiles: new ProfileQueryBuilder(),
  chats: new ChatQueryBuilder(),
  chatMembers: new ChatMemberQueryBuilder(),
  messages: new MessageQueryBuilder(),
  reactions: new ReactionQueryBuilder(),
  canvases: new CanvasQueryBuilder(),
  strokes: new StrokeQueryBuilder(),
  attachments: new AttachmentQueryBuilder(),
  wasmSchema,
};
