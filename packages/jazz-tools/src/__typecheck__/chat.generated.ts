// Hand-written in the shape `jazz-tools generate` generates, for the type tests.
import { bindApp, type AppTypes, type ComputedImpl, type TransformImpl } from "../typed-app.js";

export interface Chat {
  readonly id: string;
  name: string | null;
  isPublic: boolean;
  joinCode: string | null;
}
export interface ChatInsert {
  name?: string | null;
  isPublic: boolean;
  joinCode?: string | null;
}
export interface Message {
  readonly id: string;
  chatId: string;
  text: string;
  senderId: string;
  readonly preview: string;
  sentAt: string;
  tags: string[];
}
export interface MessageInsert {
  chatId: string;
  text: string;
  senderId: string;
  sentAt: string;
  tags?: string[];
}
export interface Profile {
  readonly id: string;
  name: string;
  avatar: string | null;
}
export interface ProfileInsert {
  name: string;
  avatar?: string | null;
}

interface Types extends AppTypes {
  chats: {
    row: Chat;
    insert: ChatInsert;
    columns: {
      name: { kind: "string"; nullable: true; value: string };
      isPublic: { kind: "boolean"; nullable: false; value: boolean };
      joinCode: { kind: "string"; nullable: true; value: string };
    };
    relations: { messagesViaChat: { many: "messages" } };
  };
  messages: {
    row: Message;
    insert: MessageInsert;
    columns: {
      chatId: { kind: "uuid"; nullable: false; value: string };
      text: { kind: "string"; nullable: false; value: string };
      senderId: { kind: "uuid"; nullable: false; value: string };
      sentAt: { kind: "timestamp"; nullable: false; value: Date };
      tags: { kind: "array"; nullable: false; value: string[] };
    };
    relations: {
      chat: { one: "chats"; nullable: false };
      sender: { one: "profiles"; nullable: false };
    };
    computed: { preview: { from: { text: string }; value: string } };
    transforms: { sentAt: "isoDate" };
  };
  profiles: {
    row: Profile;
    insert: ProfileInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      avatar: { kind: "string"; nullable: true; value: string };
    };
    relations: { messagesViaSender: { many: "messages" } };
  };
}
interface Transforms {
  isoDate: { stored: Date; view: string };
}
export type AppComputed = ComputedImpl<Types>;
export type AppTransforms = TransformImpl<Transforms, Types>;

declare const computed: AppComputed;
declare const transforms: AppTransforms;

export const app = bindApp<Types, Transforms>({
  source: "sha256:0000000000000000000000000000000000000000000000000000000000000000",
  schema: { current: "x", schemas: [], migrations: [] },
  computed,
  transforms,
});
