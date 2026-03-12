// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";
export type JsonValue = string | number | boolean | null | { [key: string]: JsonValue } | JsonValue[];

export type PermissionIntrospectionColumn = "$canRead" | "$canEdit" | "$canDelete";
export interface PermissionIntrospectionColumns {
  $canRead: boolean | null;
  $canEdit: boolean | null;
  $canDelete: boolean | null;
}

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
  joinCode?: string;
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
  joinCode?: string;
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
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface ChatWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  isPublic?: boolean;
  createdBy?: string | { eq?: string; ne?: string; contains?: string };
  joinCode?: string | { eq?: string; ne?: string; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface ChatMemberWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  chat?: string | { eq?: string; ne?: string };
  userId?: string | { eq?: string; ne?: string; contains?: string };
  joinCode?: string | { eq?: string; ne?: string; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface MessageWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  chat?: string | { eq?: string; ne?: string };
  text?: string | { eq?: string; ne?: string; contains?: string };
  sender?: string | { eq?: string; ne?: string };
  senderId?: string | { eq?: string; ne?: string; contains?: string };
  createdAt?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface ReactionWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  message?: string | { eq?: string; ne?: string };
  userId?: string | { eq?: string; ne?: string; contains?: string };
  emoji?: string | { eq?: string; ne?: string; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface CanvasWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  chat?: string | { eq?: string; ne?: string };
  createdAt?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface StrokeWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  canvas?: string | { eq?: string; ne?: string };
  ownerId?: string | { eq?: string; ne?: string; contains?: string };
  color?: string | { eq?: string; ne?: string; contains?: string };
  width?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  pointsJson?: string | { eq?: string; ne?: string; contains?: string };
  createdAt?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface AttachmentWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  message?: string | { eq?: string; ne?: string };
  type?: string | { eq?: string; ne?: string; contains?: string };
  name?: string | { eq?: string; ne?: string; contains?: string };
  data?: string | { eq?: string; ne?: string; contains?: string };
  mimeType?: string | { eq?: string; ne?: string; contains?: string };
  size?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

type AnyProfileQueryBuilder<T = any> = { readonly _table: "profiles" } & QueryBuilder<T>;
type AnyChatQueryBuilder<T = any> = { readonly _table: "chats" } & QueryBuilder<T>;
type AnyChatMemberQueryBuilder<T = any> = { readonly _table: "chatMembers" } & QueryBuilder<T>;
type AnyMessageQueryBuilder<T = any> = { readonly _table: "messages" } & QueryBuilder<T>;
type AnyReactionQueryBuilder<T = any> = { readonly _table: "reactions" } & QueryBuilder<T>;
type AnyCanvasQueryBuilder<T = any> = { readonly _table: "canvases" } & QueryBuilder<T>;
type AnyStrokeQueryBuilder<T = any> = { readonly _table: "strokes" } & QueryBuilder<T>;
type AnyAttachmentQueryBuilder<T = any> = { readonly _table: "attachments" } & QueryBuilder<T>;

export interface ProfileInclude {
  messagesViaSender?: true | MessageInclude | AnyMessageQueryBuilder<any>;
}

export interface ChatInclude {
  chatMembersViaChat?: true | ChatMemberInclude | AnyChatMemberQueryBuilder<any>;
  messagesViaChat?: true | MessageInclude | AnyMessageQueryBuilder<any>;
  canvasesViaChat?: true | CanvasInclude | AnyCanvasQueryBuilder<any>;
}

export interface ChatMemberInclude {
  chat?: true | ChatInclude | AnyChatQueryBuilder<any>;
}

export interface MessageInclude {
  chat?: true | ChatInclude | AnyChatQueryBuilder<any>;
  sender?: true | ProfileInclude | AnyProfileQueryBuilder<any>;
  reactionsViaMessage?: true | ReactionInclude | AnyReactionQueryBuilder<any>;
  attachmentsViaMessage?: true | AttachmentInclude | AnyAttachmentQueryBuilder<any>;
}

export interface ReactionInclude {
  message?: true | MessageInclude | AnyMessageQueryBuilder<any>;
}

export interface CanvasInclude {
  chat?: true | ChatInclude | AnyChatQueryBuilder<any>;
  strokesViaCanvas?: true | StrokeInclude | AnyStrokeQueryBuilder<any>;
}

export interface StrokeInclude {
  canvas?: true | CanvasInclude | AnyCanvasQueryBuilder<any>;
}

export interface AttachmentInclude {
  message?: true | MessageInclude | AnyMessageQueryBuilder<any>;
}

export type ProfileIncludedRelations<I extends ProfileInclude = {}> = {
  [K in keyof I]-?:
    K extends "messagesViaSender"
      ? NonNullable<I["messagesViaSender"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Message[]
          : RelationInclude extends AnyMessageQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends MessageInclude
              ? MessageWithIncludes<RelationInclude>[]
              : never
        : never
    : never;
};

export type ChatIncludedRelations<I extends ChatInclude = {}> = {
  [K in keyof I]-?:
    K extends "chatMembersViaChat"
      ? NonNullable<I["chatMembersViaChat"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? ChatMember[]
          : RelationInclude extends AnyChatMemberQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends ChatMemberInclude
              ? ChatMemberWithIncludes<RelationInclude>[]
              : never
        : never
    : K extends "messagesViaChat"
      ? NonNullable<I["messagesViaChat"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Message[]
          : RelationInclude extends AnyMessageQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends MessageInclude
              ? MessageWithIncludes<RelationInclude>[]
              : never
        : never
    : K extends "canvasesViaChat"
      ? NonNullable<I["canvasesViaChat"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Canvas[]
          : RelationInclude extends AnyCanvasQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends CanvasInclude
              ? CanvasWithIncludes<RelationInclude>[]
              : never
        : never
    : never;
};

export type ChatMemberIncludedRelations<I extends ChatMemberInclude = {}> = {
  [K in keyof I]-?:
    K extends "chat"
      ? NonNullable<I["chat"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Chat
          : RelationInclude extends AnyChatQueryBuilder<infer QueryRow>
            ? QueryRow
            : RelationInclude extends ChatInclude
              ? ChatWithIncludes<RelationInclude>
              : never
        : never
    : never;
};

export type MessageIncludedRelations<I extends MessageInclude = {}> = {
  [K in keyof I]-?:
    K extends "chat"
      ? NonNullable<I["chat"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Chat
          : RelationInclude extends AnyChatQueryBuilder<infer QueryRow>
            ? QueryRow
            : RelationInclude extends ChatInclude
              ? ChatWithIncludes<RelationInclude>
              : never
        : never
    : K extends "sender"
      ? NonNullable<I["sender"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Profile
          : RelationInclude extends AnyProfileQueryBuilder<infer QueryRow>
            ? QueryRow
            : RelationInclude extends ProfileInclude
              ? ProfileWithIncludes<RelationInclude>
              : never
        : never
    : K extends "reactionsViaMessage"
      ? NonNullable<I["reactionsViaMessage"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Reaction[]
          : RelationInclude extends AnyReactionQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends ReactionInclude
              ? ReactionWithIncludes<RelationInclude>[]
              : never
        : never
    : K extends "attachmentsViaMessage"
      ? NonNullable<I["attachmentsViaMessage"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Attachment[]
          : RelationInclude extends AnyAttachmentQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends AttachmentInclude
              ? AttachmentWithIncludes<RelationInclude>[]
              : never
        : never
    : never;
};

export type ReactionIncludedRelations<I extends ReactionInclude = {}> = {
  [K in keyof I]-?:
    K extends "message"
      ? NonNullable<I["message"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Message
          : RelationInclude extends AnyMessageQueryBuilder<infer QueryRow>
            ? QueryRow
            : RelationInclude extends MessageInclude
              ? MessageWithIncludes<RelationInclude>
              : never
        : never
    : never;
};

export type CanvasIncludedRelations<I extends CanvasInclude = {}> = {
  [K in keyof I]-?:
    K extends "chat"
      ? NonNullable<I["chat"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Chat
          : RelationInclude extends AnyChatQueryBuilder<infer QueryRow>
            ? QueryRow
            : RelationInclude extends ChatInclude
              ? ChatWithIncludes<RelationInclude>
              : never
        : never
    : K extends "strokesViaCanvas"
      ? NonNullable<I["strokesViaCanvas"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Stroke[]
          : RelationInclude extends AnyStrokeQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends StrokeInclude
              ? StrokeWithIncludes<RelationInclude>[]
              : never
        : never
    : never;
};

export type StrokeIncludedRelations<I extends StrokeInclude = {}> = {
  [K in keyof I]-?:
    K extends "canvas"
      ? NonNullable<I["canvas"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Canvas
          : RelationInclude extends AnyCanvasQueryBuilder<infer QueryRow>
            ? QueryRow
            : RelationInclude extends CanvasInclude
              ? CanvasWithIncludes<RelationInclude>
              : never
        : never
    : never;
};

export type AttachmentIncludedRelations<I extends AttachmentInclude = {}> = {
  [K in keyof I]-?:
    K extends "message"
      ? NonNullable<I["message"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Message
          : RelationInclude extends AnyMessageQueryBuilder<infer QueryRow>
            ? QueryRow
            : RelationInclude extends MessageInclude
              ? MessageWithIncludes<RelationInclude>
              : never
        : never
    : never;
};

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

export type ProfileWithIncludes<I extends ProfileInclude = {}> = Omit<Profile, Extract<keyof I, keyof Profile>> & ProfileIncludedRelations<I>;

export type ChatWithIncludes<I extends ChatInclude = {}> = Omit<Chat, Extract<keyof I, keyof Chat>> & ChatIncludedRelations<I>;

export type ChatMemberWithIncludes<I extends ChatMemberInclude = {}> = Omit<ChatMember, Extract<keyof I, keyof ChatMember>> & ChatMemberIncludedRelations<I>;

export type MessageWithIncludes<I extends MessageInclude = {}> = Omit<Message, Extract<keyof I, keyof Message>> & MessageIncludedRelations<I>;

export type ReactionWithIncludes<I extends ReactionInclude = {}> = Omit<Reaction, Extract<keyof I, keyof Reaction>> & ReactionIncludedRelations<I>;

export type CanvasWithIncludes<I extends CanvasInclude = {}> = Omit<Canvas, Extract<keyof I, keyof Canvas>> & CanvasIncludedRelations<I>;

export type StrokeWithIncludes<I extends StrokeInclude = {}> = Omit<Stroke, Extract<keyof I, keyof Stroke>> & StrokeIncludedRelations<I>;

export type AttachmentWithIncludes<I extends AttachmentInclude = {}> = Omit<Attachment, Extract<keyof I, keyof Attachment>> & AttachmentIncludedRelations<I>;

export type ProfileSelectableColumn = keyof Profile | PermissionIntrospectionColumn | "*";
export type ProfileOrderableColumn = keyof Profile | PermissionIntrospectionColumn;

export type ProfileSelected<S extends ProfileSelectableColumn = keyof Profile> = "*" extends S ? Profile : Pick<Profile, Extract<S | "id", keyof Profile>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type ProfileSelectedWithIncludes<I extends ProfileInclude = {}, S extends ProfileSelectableColumn = keyof Profile> = Omit<ProfileSelected<S>, Extract<keyof I, keyof ProfileSelected<S>>> & ProfileIncludedRelations<I>;

export type ChatSelectableColumn = keyof Chat | PermissionIntrospectionColumn | "*";
export type ChatOrderableColumn = keyof Chat | PermissionIntrospectionColumn;

export type ChatSelected<S extends ChatSelectableColumn = keyof Chat> = "*" extends S ? Chat : Pick<Chat, Extract<S | "id", keyof Chat>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type ChatSelectedWithIncludes<I extends ChatInclude = {}, S extends ChatSelectableColumn = keyof Chat> = Omit<ChatSelected<S>, Extract<keyof I, keyof ChatSelected<S>>> & ChatIncludedRelations<I>;

export type ChatMemberSelectableColumn = keyof ChatMember | PermissionIntrospectionColumn | "*";
export type ChatMemberOrderableColumn = keyof ChatMember | PermissionIntrospectionColumn;

export type ChatMemberSelected<S extends ChatMemberSelectableColumn = keyof ChatMember> = "*" extends S ? ChatMember : Pick<ChatMember, Extract<S | "id", keyof ChatMember>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type ChatMemberSelectedWithIncludes<I extends ChatMemberInclude = {}, S extends ChatMemberSelectableColumn = keyof ChatMember> = Omit<ChatMemberSelected<S>, Extract<keyof I, keyof ChatMemberSelected<S>>> & ChatMemberIncludedRelations<I>;

export type MessageSelectableColumn = keyof Message | PermissionIntrospectionColumn | "*";
export type MessageOrderableColumn = keyof Message | PermissionIntrospectionColumn;

export type MessageSelected<S extends MessageSelectableColumn = keyof Message> = "*" extends S ? Message : Pick<Message, Extract<S | "id", keyof Message>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type MessageSelectedWithIncludes<I extends MessageInclude = {}, S extends MessageSelectableColumn = keyof Message> = Omit<MessageSelected<S>, Extract<keyof I, keyof MessageSelected<S>>> & MessageIncludedRelations<I>;

export type ReactionSelectableColumn = keyof Reaction | PermissionIntrospectionColumn | "*";
export type ReactionOrderableColumn = keyof Reaction | PermissionIntrospectionColumn;

export type ReactionSelected<S extends ReactionSelectableColumn = keyof Reaction> = "*" extends S ? Reaction : Pick<Reaction, Extract<S | "id", keyof Reaction>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type ReactionSelectedWithIncludes<I extends ReactionInclude = {}, S extends ReactionSelectableColumn = keyof Reaction> = Omit<ReactionSelected<S>, Extract<keyof I, keyof ReactionSelected<S>>> & ReactionIncludedRelations<I>;

export type CanvasSelectableColumn = keyof Canvas | PermissionIntrospectionColumn | "*";
export type CanvasOrderableColumn = keyof Canvas | PermissionIntrospectionColumn;

export type CanvasSelected<S extends CanvasSelectableColumn = keyof Canvas> = "*" extends S ? Canvas : Pick<Canvas, Extract<S | "id", keyof Canvas>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type CanvasSelectedWithIncludes<I extends CanvasInclude = {}, S extends CanvasSelectableColumn = keyof Canvas> = Omit<CanvasSelected<S>, Extract<keyof I, keyof CanvasSelected<S>>> & CanvasIncludedRelations<I>;

export type StrokeSelectableColumn = keyof Stroke | PermissionIntrospectionColumn | "*";
export type StrokeOrderableColumn = keyof Stroke | PermissionIntrospectionColumn;

export type StrokeSelected<S extends StrokeSelectableColumn = keyof Stroke> = "*" extends S ? Stroke : Pick<Stroke, Extract<S | "id", keyof Stroke>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type StrokeSelectedWithIncludes<I extends StrokeInclude = {}, S extends StrokeSelectableColumn = keyof Stroke> = Omit<StrokeSelected<S>, Extract<keyof I, keyof StrokeSelected<S>>> & StrokeIncludedRelations<I>;

export type AttachmentSelectableColumn = keyof Attachment | PermissionIntrospectionColumn | "*";
export type AttachmentOrderableColumn = keyof Attachment | PermissionIntrospectionColumn;

export type AttachmentSelected<S extends AttachmentSelectableColumn = keyof Attachment> = "*" extends S ? Attachment : Pick<Attachment, Extract<S | "id", keyof Attachment>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type AttachmentSelectedWithIncludes<I extends AttachmentInclude = {}, S extends AttachmentSelectableColumn = keyof Attachment> = Omit<AttachmentSelected<S>, Extract<keyof I, keyof AttachmentSelected<S>>> & AttachmentIncludedRelations<I>;

export const wasmSchema: WasmSchema = {
  "profiles": {
    "columns": [
      {
        "name": "userId",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "name",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "avatar",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      }
    ],
    "policies": {
      "select": {
        "using": {
          "type": "True"
        }
      },
      "insert": {
        "with_check": {
          "type": "Cmp",
          "column": "userId",
          "op": "Eq",
          "value": {
            "type": "SessionRef",
            "path": [
              "user_id"
            ]
          }
        }
      },
      "update": {
        "using": {
          "type": "Cmp",
          "column": "userId",
          "op": "Eq",
          "value": {
            "type": "SessionRef",
            "path": [
              "user_id"
            ]
          }
        },
        "with_check": {
          "type": "Cmp",
          "column": "userId",
          "op": "Eq",
          "value": {
            "type": "SessionRef",
            "path": [
              "user_id"
            ]
          }
        }
      },
      "delete": {}
    }
  },
  "chats": {
    "columns": [
      {
        "name": "isPublic",
        "column_type": {
          "type": "Boolean"
        },
        "nullable": false
      },
      {
        "name": "createdBy",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "joinCode",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      }
    ],
    "policies": {
      "select": {
        "using": {
          "type": "Or",
          "exprs": [
            {
              "type": "Cmp",
              "column": "isPublic",
              "op": "Eq",
              "value": {
                "type": "Literal",
                "value": {
                  "type": "Boolean",
                  "value": true
                }
              }
            },
            {
              "type": "Exists",
              "table": "chatMembers",
              "condition": {
                "type": "And",
                "exprs": [
                  {
                    "type": "Cmp",
                    "column": "chat",
                    "op": "Eq",
                    "value": {
                      "type": "SessionRef",
                      "path": [
                        "__jazz_outer_row",
                        "id"
                      ]
                    }
                  },
                  {
                    "type": "Cmp",
                    "column": "userId",
                    "op": "Eq",
                    "value": {
                      "type": "SessionRef",
                      "path": [
                        "user_id"
                      ]
                    }
                  }
                ]
              }
            },
            {
              "type": "Cmp",
              "column": "joinCode",
              "op": "Eq",
              "value": {
                "type": "SessionRef",
                "path": [
                  "claims",
                  "join_code"
                ]
              }
            }
          ]
        }
      },
      "insert": {
        "with_check": {
          "type": "Cmp",
          "column": "createdBy",
          "op": "Eq",
          "value": {
            "type": "SessionRef",
            "path": [
              "user_id"
            ]
          }
        }
      },
      "update": {},
      "delete": {}
    }
  },
  "chatMembers": {
    "columns": [
      {
        "name": "chat",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "chats"
      },
      {
        "name": "userId",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "joinCode",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      }
    ],
    "policies": {
      "select": {
        "using": {
          "type": "Cmp",
          "column": "userId",
          "op": "Eq",
          "value": {
            "type": "SessionRef",
            "path": [
              "user_id"
            ]
          }
        }
      },
      "insert": {
        "with_check": {
          "type": "Cmp",
          "column": "userId",
          "op": "Eq",
          "value": {
            "type": "SessionRef",
            "path": [
              "user_id"
            ]
          }
        }
      },
      "update": {},
      "delete": {}
    }
  },
  "messages": {
    "columns": [
      {
        "name": "chat",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "chats"
      },
      {
        "name": "text",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "sender",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "profiles"
      },
      {
        "name": "senderId",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "createdAt",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ],
    "policies": {
      "select": {
        "using": {
          "type": "Or",
          "exprs": [
            {
              "type": "Inherits",
              "operation": "Select",
              "via_column": "chat"
            },
            {
              "type": "Exists",
              "table": "chatMembers",
              "condition": {
                "type": "And",
                "exprs": [
                  {
                    "type": "Cmp",
                    "column": "chat",
                    "op": "Eq",
                    "value": {
                      "type": "SessionRef",
                      "path": [
                        "__jazz_outer_row",
                        "chat"
                      ]
                    }
                  },
                  {
                    "type": "Cmp",
                    "column": "userId",
                    "op": "Eq",
                    "value": {
                      "type": "SessionRef",
                      "path": [
                        "user_id"
                      ]
                    }
                  }
                ]
              }
            }
          ]
        }
      },
      "insert": {
        "with_check": {
          "type": "Exists",
          "table": "chatMembers",
          "condition": {
            "type": "And",
            "exprs": [
              {
                "type": "Cmp",
                "column": "chat",
                "op": "Eq",
                "value": {
                  "type": "SessionRef",
                  "path": [
                    "__jazz_outer_row",
                    "chat"
                  ]
                }
              },
              {
                "type": "Cmp",
                "column": "userId",
                "op": "Eq",
                "value": {
                  "type": "SessionRef",
                  "path": [
                    "user_id"
                  ]
                }
              }
            ]
          }
        }
      },
      "update": {},
      "delete": {
        "using": {
          "type": "Cmp",
          "column": "senderId",
          "op": "Eq",
          "value": {
            "type": "SessionRef",
            "path": [
              "user_id"
            ]
          }
        }
      }
    }
  },
  "reactions": {
    "columns": [
      {
        "name": "message",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "messages"
      },
      {
        "name": "userId",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "emoji",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      }
    ],
    "policies": {
      "select": {
        "using": {
          "type": "Inherits",
          "operation": "Select",
          "via_column": "message"
        }
      },
      "insert": {
        "with_check": {
          "type": "Cmp",
          "column": "userId",
          "op": "Eq",
          "value": {
            "type": "SessionRef",
            "path": [
              "user_id"
            ]
          }
        }
      },
      "update": {},
      "delete": {
        "using": {
          "type": "Cmp",
          "column": "userId",
          "op": "Eq",
          "value": {
            "type": "SessionRef",
            "path": [
              "user_id"
            ]
          }
        }
      }
    }
  },
  "canvases": {
    "columns": [
      {
        "name": "chat",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "chats"
      },
      {
        "name": "createdAt",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ],
    "policies": {
      "select": {
        "using": {
          "type": "Or",
          "exprs": [
            {
              "type": "Inherits",
              "operation": "Select",
              "via_column": "chat"
            },
            {
              "type": "Exists",
              "table": "chatMembers",
              "condition": {
                "type": "And",
                "exprs": [
                  {
                    "type": "Cmp",
                    "column": "chat",
                    "op": "Eq",
                    "value": {
                      "type": "SessionRef",
                      "path": [
                        "__jazz_outer_row",
                        "chat"
                      ]
                    }
                  },
                  {
                    "type": "Cmp",
                    "column": "userId",
                    "op": "Eq",
                    "value": {
                      "type": "SessionRef",
                      "path": [
                        "user_id"
                      ]
                    }
                  }
                ]
              }
            }
          ]
        }
      },
      "insert": {
        "with_check": {
          "type": "Exists",
          "table": "chatMembers",
          "condition": {
            "type": "And",
            "exprs": [
              {
                "type": "Cmp",
                "column": "chat",
                "op": "Eq",
                "value": {
                  "type": "SessionRef",
                  "path": [
                    "__jazz_outer_row",
                    "chat"
                  ]
                }
              },
              {
                "type": "Cmp",
                "column": "userId",
                "op": "Eq",
                "value": {
                  "type": "SessionRef",
                  "path": [
                    "user_id"
                  ]
                }
              }
            ]
          }
        }
      },
      "update": {},
      "delete": {}
    }
  },
  "strokes": {
    "columns": [
      {
        "name": "canvas",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "canvases"
      },
      {
        "name": "ownerId",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "color",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "width",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "pointsJson",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "createdAt",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ],
    "policies": {
      "select": {
        "using": {
          "type": "Inherits",
          "operation": "Select",
          "via_column": "canvas"
        }
      },
      "insert": {
        "with_check": {
          "type": "Inherits",
          "operation": "Select",
          "via_column": "canvas"
        }
      },
      "update": {},
      "delete": {
        "using": {
          "type": "Cmp",
          "column": "ownerId",
          "op": "Eq",
          "value": {
            "type": "SessionRef",
            "path": [
              "user_id"
            ]
          }
        }
      }
    }
  },
  "attachments": {
    "columns": [
      {
        "name": "message",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "messages"
      },
      {
        "name": "type",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "name",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "data",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "mimeType",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "size",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      }
    ],
    "policies": {
      "select": {
        "using": {
          "type": "Inherits",
          "operation": "Select",
          "via_column": "message"
        }
      },
      "insert": {
        "with_check": {
          "type": "Inherits",
          "operation": "Select",
          "via_column": "message"
        }
      },
      "update": {},
      "delete": {}
    }
  }
};

export class ProfileQueryBuilder<I extends ProfileInclude = {}, S extends ProfileSelectableColumn = keyof Profile> implements QueryBuilder<ProfileSelectedWithIncludes<I, S>> {
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

  select<NewS extends ProfileSelectableColumn>(...columns: [NewS, ...NewS[]]): ProfileQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ProfileInclude>(relations: NewI): ProfileQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: ProfileOrderableColumn, direction: "asc" | "desc" = "asc"): ProfileQueryBuilder<I, S> {
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
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
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
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
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

  private _clone<CloneI extends ProfileInclude = I, CloneS extends ProfileSelectableColumn = S>(): ProfileQueryBuilder<CloneI, CloneS> {
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

export class ChatQueryBuilder<I extends ChatInclude = {}, S extends ChatSelectableColumn = keyof Chat> implements QueryBuilder<ChatSelectedWithIncludes<I, S>> {
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

  select<NewS extends ChatSelectableColumn>(...columns: [NewS, ...NewS[]]): ChatQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ChatInclude>(relations: NewI): ChatQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: ChatOrderableColumn, direction: "asc" | "desc" = "asc"): ChatQueryBuilder<I, S> {
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

  hopTo(relation: "chatMembersViaChat" | "messagesViaChat" | "canvasesViaChat"): ChatQueryBuilder<I, S> {
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
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
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
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
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

  private _clone<CloneI extends ChatInclude = I, CloneS extends ChatSelectableColumn = S>(): ChatQueryBuilder<CloneI, CloneS> {
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

export class ChatMemberQueryBuilder<I extends ChatMemberInclude = {}, S extends ChatMemberSelectableColumn = keyof ChatMember> implements QueryBuilder<ChatMemberSelectedWithIncludes<I, S>> {
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

  select<NewS extends ChatMemberSelectableColumn>(...columns: [NewS, ...NewS[]]): ChatMemberQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ChatMemberInclude>(relations: NewI): ChatMemberQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: ChatMemberOrderableColumn, direction: "asc" | "desc" = "asc"): ChatMemberQueryBuilder<I, S> {
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
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
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
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
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

  private _clone<CloneI extends ChatMemberInclude = I, CloneS extends ChatMemberSelectableColumn = S>(): ChatMemberQueryBuilder<CloneI, CloneS> {
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

export class MessageQueryBuilder<I extends MessageInclude = {}, S extends MessageSelectableColumn = keyof Message> implements QueryBuilder<MessageSelectedWithIncludes<I, S>> {
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

  select<NewS extends MessageSelectableColumn>(...columns: [NewS, ...NewS[]]): MessageQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends MessageInclude>(relations: NewI): MessageQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: MessageOrderableColumn, direction: "asc" | "desc" = "asc"): MessageQueryBuilder<I, S> {
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

  hopTo(relation: "chat" | "sender" | "reactionsViaMessage" | "attachmentsViaMessage"): MessageQueryBuilder<I, S> {
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
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
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
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
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

  private _clone<CloneI extends MessageInclude = I, CloneS extends MessageSelectableColumn = S>(): MessageQueryBuilder<CloneI, CloneS> {
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

export class ReactionQueryBuilder<I extends ReactionInclude = {}, S extends ReactionSelectableColumn = keyof Reaction> implements QueryBuilder<ReactionSelectedWithIncludes<I, S>> {
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

  select<NewS extends ReactionSelectableColumn>(...columns: [NewS, ...NewS[]]): ReactionQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ReactionInclude>(relations: NewI): ReactionQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: ReactionOrderableColumn, direction: "asc" | "desc" = "asc"): ReactionQueryBuilder<I, S> {
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
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
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
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
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

  private _clone<CloneI extends ReactionInclude = I, CloneS extends ReactionSelectableColumn = S>(): ReactionQueryBuilder<CloneI, CloneS> {
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

export class CanvasQueryBuilder<I extends CanvasInclude = {}, S extends CanvasSelectableColumn = keyof Canvas> implements QueryBuilder<CanvasSelectedWithIncludes<I, S>> {
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

  select<NewS extends CanvasSelectableColumn>(...columns: [NewS, ...NewS[]]): CanvasQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends CanvasInclude>(relations: NewI): CanvasQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: CanvasOrderableColumn, direction: "asc" | "desc" = "asc"): CanvasQueryBuilder<I, S> {
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
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
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
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
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

  private _clone<CloneI extends CanvasInclude = I, CloneS extends CanvasSelectableColumn = S>(): CanvasQueryBuilder<CloneI, CloneS> {
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

export class StrokeQueryBuilder<I extends StrokeInclude = {}, S extends StrokeSelectableColumn = keyof Stroke> implements QueryBuilder<StrokeSelectedWithIncludes<I, S>> {
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

  select<NewS extends StrokeSelectableColumn>(...columns: [NewS, ...NewS[]]): StrokeQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends StrokeInclude>(relations: NewI): StrokeQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: StrokeOrderableColumn, direction: "asc" | "desc" = "asc"): StrokeQueryBuilder<I, S> {
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
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
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
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
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

  private _clone<CloneI extends StrokeInclude = I, CloneS extends StrokeSelectableColumn = S>(): StrokeQueryBuilder<CloneI, CloneS> {
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

export class AttachmentQueryBuilder<I extends AttachmentInclude = {}, S extends AttachmentSelectableColumn = keyof Attachment> implements QueryBuilder<AttachmentSelectedWithIncludes<I, S>> {
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

  select<NewS extends AttachmentSelectableColumn>(...columns: [NewS, ...NewS[]]): AttachmentQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends AttachmentInclude>(relations: NewI): AttachmentQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: AttachmentOrderableColumn, direction: "asc" | "desc" = "asc"): AttachmentQueryBuilder<I, S> {
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
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
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
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
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

  private _clone<CloneI extends AttachmentInclude = I, CloneS extends AttachmentSelectableColumn = S>(): AttachmentQueryBuilder<CloneI, CloneS> {
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
