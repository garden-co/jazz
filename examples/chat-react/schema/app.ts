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
  chatId: string;
  userId: string;
  joinCode?: string;
}

export interface Message {
  id: string;
  chatId: string;
  text: string;
  senderId: string;
  createdAt: Date;
}

export interface Reaction {
  id: string;
  messageId: string;
  userId: string;
  emoji: string;
}

export interface Canvas {
  id: string;
  chatId: string;
  createdAt: Date;
}

export interface Stroke {
  id: string;
  canvasId: string;
  ownerId: string;
  color: string;
  width: number;
  pointsJson: string;
  createdAt: Date;
}

export interface Attachment {
  id: string;
  messageId: string;
  type: string;
  name: string;
  fileId: string;
  size: number;
}

export interface FilePart {
  id: string;
  data: Uint8Array;
}

export interface File {
  id: string;
  name?: string;
  mimeType: string;
  partIds: string[];
  partSizes: number[];
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
  chatId: string;
  userId: string;
  joinCode?: string;
}

export interface MessageInit {
  chatId: string;
  text: string;
  senderId: string;
  createdAt: Date;
}

export interface ReactionInit {
  messageId: string;
  userId: string;
  emoji: string;
}

export interface CanvasInit {
  chatId: string;
  createdAt: Date;
}

export interface StrokeInit {
  canvasId: string;
  ownerId: string;
  color: string;
  width: number;
  pointsJson: string;
  createdAt: Date;
}

export interface AttachmentInit {
  messageId: string;
  type: string;
  name: string;
  fileId: string;
  size: number;
}

export interface FilePartInit {
  data: Uint8Array;
}

export interface FileInit {
  name?: string;
  mimeType: string;
  partIds: string[];
  partSizes: number[];
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
  chatId?: string | { eq?: string; ne?: string };
  userId?: string | { eq?: string; ne?: string; contains?: string };
  joinCode?: string | { eq?: string; ne?: string; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface MessageWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  chatId?: string | { eq?: string; ne?: string };
  text?: string | { eq?: string; ne?: string; contains?: string };
  senderId?: string | { eq?: string; ne?: string };
  createdAt?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface ReactionWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  messageId?: string | { eq?: string; ne?: string };
  userId?: string | { eq?: string; ne?: string; contains?: string };
  emoji?: string | { eq?: string; ne?: string; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface CanvasWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  chatId?: string | { eq?: string; ne?: string };
  createdAt?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface StrokeWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  canvasId?: string | { eq?: string; ne?: string };
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
  messageId?: string | { eq?: string; ne?: string };
  type?: string | { eq?: string; ne?: string; contains?: string };
  name?: string | { eq?: string; ne?: string; contains?: string };
  fileId?: string | { eq?: string; ne?: string };
  size?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface FilePartWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  data?: Uint8Array | { eq?: Uint8Array; ne?: Uint8Array };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface FileWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  name?: string | { eq?: string; ne?: string; contains?: string };
  mimeType?: string | { eq?: string; ne?: string; contains?: string };
  partIds?: string[] | { eq?: string[]; contains?: string };
  partSizes?: number[] | { eq?: number[]; contains?: number };
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
type AnyFilePartQueryBuilder<T = any> = { readonly _table: "file_parts" } & QueryBuilder<T>;
type AnyFileQueryBuilder<T = any> = { readonly _table: "files" } & QueryBuilder<T>;

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
  file?: true | FileInclude | AnyFileQueryBuilder<any>;
}

export interface FilePartInclude {
  filesViaParts?: true | FileInclude | AnyFileQueryBuilder<any>;
}

export interface FileInclude {
  attachmentsViaFile?: true | AttachmentInclude | AnyAttachmentQueryBuilder<any>;
  parts?: true | FilePartInclude | AnyFilePartQueryBuilder<any>;
}

export type ProfileIncludedRelations<I extends ProfileInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "messagesViaSender"
      ? NonNullable<I["messagesViaSender"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Message[]
          : RelationInclude extends AnyMessageQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends MessageInclude
              ? MessageWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type ChatIncludedRelations<I extends ChatInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "chatMembersViaChat"
      ? NonNullable<I["chatMembersViaChat"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? ChatMember[]
          : RelationInclude extends AnyChatMemberQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends ChatMemberInclude
              ? ChatMemberWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "messagesViaChat"
      ? NonNullable<I["messagesViaChat"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Message[]
          : RelationInclude extends AnyMessageQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends MessageInclude
              ? MessageWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "canvasesViaChat"
      ? NonNullable<I["canvasesViaChat"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Canvas[]
          : RelationInclude extends AnyCanvasQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends CanvasInclude
              ? CanvasWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type ChatMemberIncludedRelations<I extends ChatMemberInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "chat"
      ? NonNullable<I["chat"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? Chat : Chat | undefined
          : RelationInclude extends AnyChatQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends ChatInclude
              ? R extends true ? ChatWithIncludes<RelationInclude, false> : ChatWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type MessageIncludedRelations<I extends MessageInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "chat"
      ? NonNullable<I["chat"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? Chat : Chat | undefined
          : RelationInclude extends AnyChatQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends ChatInclude
              ? R extends true ? ChatWithIncludes<RelationInclude, false> : ChatWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "sender"
      ? NonNullable<I["sender"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? Profile : Profile | undefined
          : RelationInclude extends AnyProfileQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends ProfileInclude
              ? R extends true ? ProfileWithIncludes<RelationInclude, false> : ProfileWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "reactionsViaMessage"
      ? NonNullable<I["reactionsViaMessage"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Reaction[]
          : RelationInclude extends AnyReactionQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends ReactionInclude
              ? ReactionWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "attachmentsViaMessage"
      ? NonNullable<I["attachmentsViaMessage"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Attachment[]
          : RelationInclude extends AnyAttachmentQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends AttachmentInclude
              ? AttachmentWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type ReactionIncludedRelations<I extends ReactionInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "message"
      ? NonNullable<I["message"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? Message : Message | undefined
          : RelationInclude extends AnyMessageQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends MessageInclude
              ? R extends true ? MessageWithIncludes<RelationInclude, false> : MessageWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type CanvasIncludedRelations<I extends CanvasInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "chat"
      ? NonNullable<I["chat"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? Chat : Chat | undefined
          : RelationInclude extends AnyChatQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends ChatInclude
              ? R extends true ? ChatWithIncludes<RelationInclude, false> : ChatWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "strokesViaCanvas"
      ? NonNullable<I["strokesViaCanvas"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Stroke[]
          : RelationInclude extends AnyStrokeQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends StrokeInclude
              ? StrokeWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type StrokeIncludedRelations<I extends StrokeInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "canvas"
      ? NonNullable<I["canvas"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? Canvas : Canvas | undefined
          : RelationInclude extends AnyCanvasQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends CanvasInclude
              ? R extends true ? CanvasWithIncludes<RelationInclude, false> : CanvasWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type AttachmentIncludedRelations<I extends AttachmentInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "message"
      ? NonNullable<I["message"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? Message : Message | undefined
          : RelationInclude extends AnyMessageQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends MessageInclude
              ? R extends true ? MessageWithIncludes<RelationInclude, false> : MessageWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "file"
      ? NonNullable<I["file"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? File : File | undefined
          : RelationInclude extends AnyFileQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends FileInclude
              ? R extends true ? FileWithIncludes<RelationInclude, false> : FileWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type FilePartIncludedRelations<I extends FilePartInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "filesViaParts"
      ? NonNullable<I["filesViaParts"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? File[]
          : RelationInclude extends AnyFileQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends FileInclude
              ? FileWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type FileIncludedRelations<I extends FileInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "attachmentsViaFile"
      ? NonNullable<I["attachmentsViaFile"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Attachment[]
          : RelationInclude extends AnyAttachmentQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends AttachmentInclude
              ? AttachmentWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "parts"
      ? NonNullable<I["parts"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? FilePart[]
          : RelationInclude extends AnyFilePartQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends FilePartInclude
              ? FilePartWithIncludes<RelationInclude, false>[]
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
  chat: Chat | undefined;
}

export interface MessageRelations {
  chat: Chat | undefined;
  sender: Profile | undefined;
  reactionsViaMessage: Reaction[];
  attachmentsViaMessage: Attachment[];
}

export interface ReactionRelations {
  message: Message | undefined;
}

export interface CanvasRelations {
  chat: Chat | undefined;
  strokesViaCanvas: Stroke[];
}

export interface StrokeRelations {
  canvas: Canvas | undefined;
}

export interface AttachmentRelations {
  message: Message | undefined;
  file: File | undefined;
}

export interface FilePartRelations {
  filesViaParts: File[];
}

export interface FileRelations {
  attachmentsViaFile: Attachment[];
  parts: FilePart[];
}

export type ProfileWithIncludes<I extends ProfileInclude = {}, R extends boolean = false> = Profile & ProfileIncludedRelations<I, R>;

export type ChatWithIncludes<I extends ChatInclude = {}, R extends boolean = false> = Chat & ChatIncludedRelations<I, R>;

export type ChatMemberWithIncludes<I extends ChatMemberInclude = {}, R extends boolean = false> = ChatMember & ChatMemberIncludedRelations<I, R>;

export type MessageWithIncludes<I extends MessageInclude = {}, R extends boolean = false> = Message & MessageIncludedRelations<I, R>;

export type ReactionWithIncludes<I extends ReactionInclude = {}, R extends boolean = false> = Reaction & ReactionIncludedRelations<I, R>;

export type CanvasWithIncludes<I extends CanvasInclude = {}, R extends boolean = false> = Canvas & CanvasIncludedRelations<I, R>;

export type StrokeWithIncludes<I extends StrokeInclude = {}, R extends boolean = false> = Stroke & StrokeIncludedRelations<I, R>;

export type AttachmentWithIncludes<I extends AttachmentInclude = {}, R extends boolean = false> = Attachment & AttachmentIncludedRelations<I, R>;

export type FilePartWithIncludes<I extends FilePartInclude = {}, R extends boolean = false> = FilePart & FilePartIncludedRelations<I, R>;

export type FileWithIncludes<I extends FileInclude = {}, R extends boolean = false> = File & FileIncludedRelations<I, R>;

export type ProfileSelectableColumn = keyof Profile | PermissionIntrospectionColumn | "*";
export type ProfileOrderableColumn = keyof Profile | PermissionIntrospectionColumn;

export type ProfileSelected<S extends ProfileSelectableColumn = keyof Profile> = "*" extends S ? Profile : Pick<Profile, Extract<S | "id", keyof Profile>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type ProfileSelectedWithIncludes<I extends ProfileInclude = {}, S extends ProfileSelectableColumn = keyof Profile, R extends boolean = false> = ProfileSelected<S> & ProfileIncludedRelations<I, R>;

export type ChatSelectableColumn = keyof Chat | PermissionIntrospectionColumn | "*";
export type ChatOrderableColumn = keyof Chat | PermissionIntrospectionColumn;

export type ChatSelected<S extends ChatSelectableColumn = keyof Chat> = "*" extends S ? Chat : Pick<Chat, Extract<S | "id", keyof Chat>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type ChatSelectedWithIncludes<I extends ChatInclude = {}, S extends ChatSelectableColumn = keyof Chat, R extends boolean = false> = ChatSelected<S> & ChatIncludedRelations<I, R>;

export type ChatMemberSelectableColumn = keyof ChatMember | PermissionIntrospectionColumn | "*";
export type ChatMemberOrderableColumn = keyof ChatMember | PermissionIntrospectionColumn;

export type ChatMemberSelected<S extends ChatMemberSelectableColumn = keyof ChatMember> = "*" extends S ? ChatMember : Pick<ChatMember, Extract<S | "id", keyof ChatMember>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type ChatMemberSelectedWithIncludes<I extends ChatMemberInclude = {}, S extends ChatMemberSelectableColumn = keyof ChatMember, R extends boolean = false> = ChatMemberSelected<S> & ChatMemberIncludedRelations<I, R>;

export type MessageSelectableColumn = keyof Message | PermissionIntrospectionColumn | "*";
export type MessageOrderableColumn = keyof Message | PermissionIntrospectionColumn;

export type MessageSelected<S extends MessageSelectableColumn = keyof Message> = "*" extends S ? Message : Pick<Message, Extract<S | "id", keyof Message>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type MessageSelectedWithIncludes<I extends MessageInclude = {}, S extends MessageSelectableColumn = keyof Message, R extends boolean = false> = MessageSelected<S> & MessageIncludedRelations<I, R>;

export type ReactionSelectableColumn = keyof Reaction | PermissionIntrospectionColumn | "*";
export type ReactionOrderableColumn = keyof Reaction | PermissionIntrospectionColumn;

export type ReactionSelected<S extends ReactionSelectableColumn = keyof Reaction> = "*" extends S ? Reaction : Pick<Reaction, Extract<S | "id", keyof Reaction>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type ReactionSelectedWithIncludes<I extends ReactionInclude = {}, S extends ReactionSelectableColumn = keyof Reaction, R extends boolean = false> = ReactionSelected<S> & ReactionIncludedRelations<I, R>;

export type CanvasSelectableColumn = keyof Canvas | PermissionIntrospectionColumn | "*";
export type CanvasOrderableColumn = keyof Canvas | PermissionIntrospectionColumn;

export type CanvasSelected<S extends CanvasSelectableColumn = keyof Canvas> = "*" extends S ? Canvas : Pick<Canvas, Extract<S | "id", keyof Canvas>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type CanvasSelectedWithIncludes<I extends CanvasInclude = {}, S extends CanvasSelectableColumn = keyof Canvas, R extends boolean = false> = CanvasSelected<S> & CanvasIncludedRelations<I, R>;

export type StrokeSelectableColumn = keyof Stroke | PermissionIntrospectionColumn | "*";
export type StrokeOrderableColumn = keyof Stroke | PermissionIntrospectionColumn;

export type StrokeSelected<S extends StrokeSelectableColumn = keyof Stroke> = "*" extends S ? Stroke : Pick<Stroke, Extract<S | "id", keyof Stroke>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type StrokeSelectedWithIncludes<I extends StrokeInclude = {}, S extends StrokeSelectableColumn = keyof Stroke, R extends boolean = false> = StrokeSelected<S> & StrokeIncludedRelations<I, R>;

export type AttachmentSelectableColumn = keyof Attachment | PermissionIntrospectionColumn | "*";
export type AttachmentOrderableColumn = keyof Attachment | PermissionIntrospectionColumn;

export type AttachmentSelected<S extends AttachmentSelectableColumn = keyof Attachment> = "*" extends S ? Attachment : Pick<Attachment, Extract<S | "id", keyof Attachment>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type AttachmentSelectedWithIncludes<I extends AttachmentInclude = {}, S extends AttachmentSelectableColumn = keyof Attachment, R extends boolean = false> = AttachmentSelected<S> & AttachmentIncludedRelations<I, R>;

export type FilePartSelectableColumn = keyof FilePart | PermissionIntrospectionColumn | "*";
export type FilePartOrderableColumn = keyof FilePart | PermissionIntrospectionColumn;

export type FilePartSelected<S extends FilePartSelectableColumn = keyof FilePart> = "*" extends S ? FilePart : Pick<FilePart, Extract<S | "id", keyof FilePart>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type FilePartSelectedWithIncludes<I extends FilePartInclude = {}, S extends FilePartSelectableColumn = keyof FilePart, R extends boolean = false> = FilePartSelected<S> & FilePartIncludedRelations<I, R>;

export type FileSelectableColumn = keyof File | PermissionIntrospectionColumn | "*";
export type FileOrderableColumn = keyof File | PermissionIntrospectionColumn;

export type FileSelected<S extends FileSelectableColumn = keyof File> = "*" extends S ? File : Pick<File, Extract<S | "id", keyof File>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type FileSelectedWithIncludes<I extends FileInclude = {}, S extends FileSelectableColumn = keyof File, R extends boolean = false> = FileSelected<S> & FileIncludedRelations<I, R>;

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
                    "column": "chatId",
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
        "name": "chatId",
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
        "name": "chatId",
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
        "name": "senderId",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "profiles"
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
              "via_column": "chatId"
            },
            {
              "type": "Exists",
              "table": "chatMembers",
              "condition": {
                "type": "And",
                "exprs": [
                  {
                    "type": "Cmp",
                    "column": "chatId",
                    "op": "Eq",
                    "value": {
                      "type": "SessionRef",
                      "path": [
                        "__jazz_outer_row",
                        "chatId"
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
                "column": "chatId",
                "op": "Eq",
                "value": {
                  "type": "SessionRef",
                  "path": [
                    "__jazz_outer_row",
                    "chatId"
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
        "name": "messageId",
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
          "via_column": "messageId"
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
        "name": "chatId",
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
              "via_column": "chatId"
            },
            {
              "type": "Exists",
              "table": "chatMembers",
              "condition": {
                "type": "And",
                "exprs": [
                  {
                    "type": "Cmp",
                    "column": "chatId",
                    "op": "Eq",
                    "value": {
                      "type": "SessionRef",
                      "path": [
                        "__jazz_outer_row",
                        "chatId"
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
                "column": "chatId",
                "op": "Eq",
                "value": {
                  "type": "SessionRef",
                  "path": [
                    "__jazz_outer_row",
                    "chatId"
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
        "name": "canvasId",
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
          "via_column": "canvasId"
        }
      },
      "insert": {
        "with_check": {
          "type": "Inherits",
          "operation": "Select",
          "via_column": "canvasId"
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
        "name": "messageId",
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
        "name": "fileId",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "files"
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
          "via_column": "messageId"
        }
      },
      "insert": {
        "with_check": {
          "type": "Inherits",
          "operation": "Select",
          "via_column": "messageId"
        }
      },
      "update": {},
      "delete": {}
    }
  },
  "file_parts": {
    "columns": [
      {
        "name": "data",
        "column_type": {
          "type": "Bytea"
        },
        "nullable": false
      }
    ],
    "policies": {
      "select": {
        "using": {
          "type": "InheritsReferencing",
          "operation": "Select",
          "source_table": "files",
          "via_column": "partIds"
        }
      },
      "insert": {
        "with_check": {
          "type": "True"
        }
      },
      "update": {},
      "delete": {
        "using": {
          "type": "InheritsReferencing",
          "operation": "Delete",
          "source_table": "files",
          "via_column": "partIds"
        }
      }
    }
  },
  "files": {
    "columns": [
      {
        "name": "name",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "mimeType",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "partIds",
        "column_type": {
          "type": "Array",
          "element": {
            "type": "Uuid"
          }
        },
        "nullable": false,
        "references": "file_parts"
      },
      {
        "name": "partSizes",
        "column_type": {
          "type": "Array",
          "element": {
            "type": "Integer"
          }
        },
        "nullable": false
      }
    ],
    "policies": {
      "select": {
        "using": {
          "type": "InheritsReferencing",
          "operation": "Select",
          "source_table": "attachments",
          "via_column": "fileId"
        }
      },
      "insert": {
        "with_check": {
          "type": "True"
        }
      },
      "update": {},
      "delete": {
        "using": {
          "type": "InheritsReferencing",
          "operation": "Delete",
          "source_table": "attachments",
          "via_column": "fileId"
        }
      }
    }
  }
};

export class ProfileQueryBuilder<I extends ProfileInclude = {}, S extends ProfileSelectableColumn = keyof Profile, R extends boolean = false> implements QueryBuilder<ProfileSelectedWithIncludes<I, S, R>> {
  readonly _table = "profiles";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: ProfileSelectedWithIncludes<I, S, R>;
  readonly _initType!: ProfileInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<ProfileInclude> = {};
  private _requireIncludes = false;
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

  where(conditions: ProfileWhereInput): ProfileQueryBuilder<I, S, R> {
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

  select<NewS extends ProfileSelectableColumn>(...columns: [NewS, ...NewS[]]): ProfileQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ProfileInclude>(relations: NewI): ProfileQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): ProfileQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: ProfileOrderableColumn, direction: "asc" | "desc" = "asc"): ProfileQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ProfileQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ProfileQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "messagesViaSender"): ProfileQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: ProfileWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): ProfileQueryBuilder<I, S, R> {
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
      __jazz_requireIncludes: this._requireIncludes || undefined,
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

  private _clone<CloneI extends ProfileInclude = I, CloneS extends ProfileSelectableColumn = S, CloneR extends boolean = R>(): ProfileQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new ProfileQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
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

export class ChatQueryBuilder<I extends ChatInclude = {}, S extends ChatSelectableColumn = keyof Chat, R extends boolean = false> implements QueryBuilder<ChatSelectedWithIncludes<I, S, R>> {
  readonly _table = "chats";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: ChatSelectedWithIncludes<I, S, R>;
  readonly _initType!: ChatInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<ChatInclude> = {};
  private _requireIncludes = false;
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

  where(conditions: ChatWhereInput): ChatQueryBuilder<I, S, R> {
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

  select<NewS extends ChatSelectableColumn>(...columns: [NewS, ...NewS[]]): ChatQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ChatInclude>(relations: NewI): ChatQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): ChatQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: ChatOrderableColumn, direction: "asc" | "desc" = "asc"): ChatQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ChatQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ChatQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "chatMembersViaChat" | "messagesViaChat" | "canvasesViaChat"): ChatQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: ChatWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): ChatQueryBuilder<I, S, R> {
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
      __jazz_requireIncludes: this._requireIncludes || undefined,
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

  private _clone<CloneI extends ChatInclude = I, CloneS extends ChatSelectableColumn = S, CloneR extends boolean = R>(): ChatQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new ChatQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
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

export class ChatMemberQueryBuilder<I extends ChatMemberInclude = {}, S extends ChatMemberSelectableColumn = keyof ChatMember, R extends boolean = false> implements QueryBuilder<ChatMemberSelectedWithIncludes<I, S, R>> {
  readonly _table = "chatMembers";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: ChatMemberSelectedWithIncludes<I, S, R>;
  readonly _initType!: ChatMemberInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<ChatMemberInclude> = {};
  private _requireIncludes = false;
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

  where(conditions: ChatMemberWhereInput): ChatMemberQueryBuilder<I, S, R> {
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

  select<NewS extends ChatMemberSelectableColumn>(...columns: [NewS, ...NewS[]]): ChatMemberQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ChatMemberInclude>(relations: NewI): ChatMemberQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): ChatMemberQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: ChatMemberOrderableColumn, direction: "asc" | "desc" = "asc"): ChatMemberQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ChatMemberQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ChatMemberQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "chat"): ChatMemberQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: ChatMemberWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): ChatMemberQueryBuilder<I, S, R> {
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
      __jazz_requireIncludes: this._requireIncludes || undefined,
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

  private _clone<CloneI extends ChatMemberInclude = I, CloneS extends ChatMemberSelectableColumn = S, CloneR extends boolean = R>(): ChatMemberQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new ChatMemberQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
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

export class MessageQueryBuilder<I extends MessageInclude = {}, S extends MessageSelectableColumn = keyof Message, R extends boolean = false> implements QueryBuilder<MessageSelectedWithIncludes<I, S, R>> {
  readonly _table = "messages";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: MessageSelectedWithIncludes<I, S, R>;
  readonly _initType!: MessageInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<MessageInclude> = {};
  private _requireIncludes = false;
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

  where(conditions: MessageWhereInput): MessageQueryBuilder<I, S, R> {
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

  select<NewS extends MessageSelectableColumn>(...columns: [NewS, ...NewS[]]): MessageQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends MessageInclude>(relations: NewI): MessageQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): MessageQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: MessageOrderableColumn, direction: "asc" | "desc" = "asc"): MessageQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): MessageQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): MessageQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "chat" | "sender" | "reactionsViaMessage" | "attachmentsViaMessage"): MessageQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: MessageWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): MessageQueryBuilder<I, S, R> {
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
      __jazz_requireIncludes: this._requireIncludes || undefined,
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

  private _clone<CloneI extends MessageInclude = I, CloneS extends MessageSelectableColumn = S, CloneR extends boolean = R>(): MessageQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new MessageQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
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

export class ReactionQueryBuilder<I extends ReactionInclude = {}, S extends ReactionSelectableColumn = keyof Reaction, R extends boolean = false> implements QueryBuilder<ReactionSelectedWithIncludes<I, S, R>> {
  readonly _table = "reactions";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: ReactionSelectedWithIncludes<I, S, R>;
  readonly _initType!: ReactionInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<ReactionInclude> = {};
  private _requireIncludes = false;
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

  where(conditions: ReactionWhereInput): ReactionQueryBuilder<I, S, R> {
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

  select<NewS extends ReactionSelectableColumn>(...columns: [NewS, ...NewS[]]): ReactionQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ReactionInclude>(relations: NewI): ReactionQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): ReactionQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: ReactionOrderableColumn, direction: "asc" | "desc" = "asc"): ReactionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ReactionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ReactionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "message"): ReactionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: ReactionWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): ReactionQueryBuilder<I, S, R> {
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
      __jazz_requireIncludes: this._requireIncludes || undefined,
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

  private _clone<CloneI extends ReactionInclude = I, CloneS extends ReactionSelectableColumn = S, CloneR extends boolean = R>(): ReactionQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new ReactionQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
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

export class CanvasQueryBuilder<I extends CanvasInclude = {}, S extends CanvasSelectableColumn = keyof Canvas, R extends boolean = false> implements QueryBuilder<CanvasSelectedWithIncludes<I, S, R>> {
  readonly _table = "canvases";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: CanvasSelectedWithIncludes<I, S, R>;
  readonly _initType!: CanvasInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<CanvasInclude> = {};
  private _requireIncludes = false;
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

  where(conditions: CanvasWhereInput): CanvasQueryBuilder<I, S, R> {
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

  select<NewS extends CanvasSelectableColumn>(...columns: [NewS, ...NewS[]]): CanvasQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends CanvasInclude>(relations: NewI): CanvasQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): CanvasQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: CanvasOrderableColumn, direction: "asc" | "desc" = "asc"): CanvasQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): CanvasQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): CanvasQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "chat" | "strokesViaCanvas"): CanvasQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: CanvasWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): CanvasQueryBuilder<I, S, R> {
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
      __jazz_requireIncludes: this._requireIncludes || undefined,
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

  private _clone<CloneI extends CanvasInclude = I, CloneS extends CanvasSelectableColumn = S, CloneR extends boolean = R>(): CanvasQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new CanvasQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
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

export class StrokeQueryBuilder<I extends StrokeInclude = {}, S extends StrokeSelectableColumn = keyof Stroke, R extends boolean = false> implements QueryBuilder<StrokeSelectedWithIncludes<I, S, R>> {
  readonly _table = "strokes";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: StrokeSelectedWithIncludes<I, S, R>;
  readonly _initType!: StrokeInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<StrokeInclude> = {};
  private _requireIncludes = false;
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

  where(conditions: StrokeWhereInput): StrokeQueryBuilder<I, S, R> {
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

  select<NewS extends StrokeSelectableColumn>(...columns: [NewS, ...NewS[]]): StrokeQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends StrokeInclude>(relations: NewI): StrokeQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): StrokeQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: StrokeOrderableColumn, direction: "asc" | "desc" = "asc"): StrokeQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): StrokeQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): StrokeQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "canvas"): StrokeQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: StrokeWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): StrokeQueryBuilder<I, S, R> {
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
      __jazz_requireIncludes: this._requireIncludes || undefined,
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

  private _clone<CloneI extends StrokeInclude = I, CloneS extends StrokeSelectableColumn = S, CloneR extends boolean = R>(): StrokeQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new StrokeQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
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

export class AttachmentQueryBuilder<I extends AttachmentInclude = {}, S extends AttachmentSelectableColumn = keyof Attachment, R extends boolean = false> implements QueryBuilder<AttachmentSelectedWithIncludes<I, S, R>> {
  readonly _table = "attachments";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: AttachmentSelectedWithIncludes<I, S, R>;
  readonly _initType!: AttachmentInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<AttachmentInclude> = {};
  private _requireIncludes = false;
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

  where(conditions: AttachmentWhereInput): AttachmentQueryBuilder<I, S, R> {
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

  select<NewS extends AttachmentSelectableColumn>(...columns: [NewS, ...NewS[]]): AttachmentQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends AttachmentInclude>(relations: NewI): AttachmentQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): AttachmentQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: AttachmentOrderableColumn, direction: "asc" | "desc" = "asc"): AttachmentQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): AttachmentQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): AttachmentQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "message" | "file"): AttachmentQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: AttachmentWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): AttachmentQueryBuilder<I, S, R> {
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
      __jazz_requireIncludes: this._requireIncludes || undefined,
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

  private _clone<CloneI extends AttachmentInclude = I, CloneS extends AttachmentSelectableColumn = S, CloneR extends boolean = R>(): AttachmentQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new AttachmentQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
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

export class FilePartQueryBuilder<I extends FilePartInclude = {}, S extends FilePartSelectableColumn = keyof FilePart, R extends boolean = false> implements QueryBuilder<FilePartSelectedWithIncludes<I, S, R>> {
  readonly _table = "file_parts";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: FilePartSelectedWithIncludes<I, S, R>;
  readonly _initType!: FilePartInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<FilePartInclude> = {};
  private _requireIncludes = false;
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

  where(conditions: FilePartWhereInput): FilePartQueryBuilder<I, S, R> {
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

  select<NewS extends FilePartSelectableColumn>(...columns: [NewS, ...NewS[]]): FilePartQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends FilePartInclude>(relations: NewI): FilePartQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): FilePartQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: FilePartOrderableColumn, direction: "asc" | "desc" = "asc"): FilePartQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): FilePartQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): FilePartQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "filesViaParts"): FilePartQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: FilePartWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): FilePartQueryBuilder<I, S, R> {
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
      __jazz_requireIncludes: this._requireIncludes || undefined,
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

  private _clone<CloneI extends FilePartInclude = I, CloneS extends FilePartSelectableColumn = S, CloneR extends boolean = R>(): FilePartQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new FilePartQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
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

export class FileQueryBuilder<I extends FileInclude = {}, S extends FileSelectableColumn = keyof File, R extends boolean = false> implements QueryBuilder<FileSelectedWithIncludes<I, S, R>> {
  readonly _table = "files";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: FileSelectedWithIncludes<I, S, R>;
  readonly _initType!: FileInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<FileInclude> = {};
  private _requireIncludes = false;
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

  where(conditions: FileWhereInput): FileQueryBuilder<I, S, R> {
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

  select<NewS extends FileSelectableColumn>(...columns: [NewS, ...NewS[]]): FileQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends FileInclude>(relations: NewI): FileQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): FileQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: FileOrderableColumn, direction: "asc" | "desc" = "asc"): FileQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): FileQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): FileQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "attachmentsViaFile" | "parts"): FileQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: FileWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): FileQueryBuilder<I, S, R> {
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
      __jazz_requireIncludes: this._requireIncludes || undefined,
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

  private _clone<CloneI extends FileInclude = I, CloneS extends FileSelectableColumn = S, CloneR extends boolean = R>(): FileQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new FileQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
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
  file_parts: FilePartQueryBuilder;
  files: FileQueryBuilder;
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
  file_parts: new FilePartQueryBuilder(),
  files: new FileQueryBuilder(),
  wasmSchema,
};
