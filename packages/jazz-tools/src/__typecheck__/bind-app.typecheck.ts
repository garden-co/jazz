import type { Db } from "../runtime/db.js";
import type { RowOf } from "../typed-app.js";
import { bindApp, type AppTypes, type ComputedImpl } from "../typed-app.js";
import { app, type AppComputed, type Chat } from "./chat.generated.js";

declare const db: Db;

// Rows come from the generated interfaces.
const chatRow: RowOf<typeof app.chats> = null as never;
const name: string | null = chatRow.name;
const id: string = chatRow.id;
String(name + id);
// @ts-expect-error unknown columns are not on the row
String(chatRow.missing);

// Inserts take the generated insert types.
db.insert(app.chats, { isPublic: true });
db.insert(app.messages, { chatId: "c", text: "t", senderId: "s", sentAt: "2026-01-01" });
// @ts-expect-error required columns must be given
db.insert(app.chats, {});
// @ts-expect-error computed fields are not insert inputs
db.insert(app.messages, { chatId: "c", text: "t", senderId: "s", sentAt: "x", preview: "p" });

// where reads the stored kinds.
app.chats.where({ name: { contains: "x" }, isPublic: true });
app.chats.where({ joinCode: null });
app.messages.where({ chatId: { in: ["a"] }, tags: { contains: "x" } });
app.messages.where({ sentAt: { gt: new Date() } });
// @ts-expect-error uuid columns take no contains
app.messages.where({ chatId: { contains: "x" } });
// @ts-expect-error non-nullable columns take no null
app.chats.where({ isPublic: null });
// @ts-expect-error computed fields are not stored filters
app.messages.where({ preview: "p" });
app.messages.orderBy("text", "desc");
app.messages.select("preview");

// include types follow the relations, and survive into hooks' rows.
const withMessages = app.chats.include({ messagesViaChat: { sender: true } });
type WithMessages = RowOf<typeof withMessages>;
const included: WithMessages = null as never;
const firstSender: string | undefined = included.messagesViaChat[0]?.sender?.name;
String(firstSender);
const required = app.messages.include({ chat: true }).requireIncludes();
const requiredChat: Chat = (null as never as RowOf<typeof required>).chat;
String(requiredChat);
// @ts-expect-error unknown relations are refused
app.chats.include({ nope: true });

// hopTo moves to the target table.
const hopped = app.messages.hopTo("chat");
const hoppedRow: RowOf<typeof hopped> = null as never;
const isPublic: boolean = hoppedRow.isPublic;
String(isPublic);

// Computed implementations are typed from the declarations.
const impl: AppComputed = { messages: { preview: (row) => row.text.slice(0, 10) } };
String(impl);
// @ts-expect-error a missing implementation is a compile error
const missing: AppComputed = { messages: {} };
String(missing);

// An app with computed fields can't be bound without them.
interface WithComputed extends AppTypes {
  items: {
    row: { readonly id: string; readonly label: string };
    insert: {};
    columns: {};
    relations: {};
    computed: { label: { from: {}; value: string } };
  };
}
const computedImpl: ComputedImpl<WithComputed> = { items: { label: () => "x" } };
bindApp<WithComputed>({
  source: "sha256:00",
  schema: { current: "x", schemas: [], migrations: [] },
  computed: computedImpl,
});
// @ts-expect-error computed is required when the app declares computed fields
bindApp<WithComputed>({
  source: "sha256:00",
  schema: { current: "x", schemas: [], migrations: [] },
});

// Hooks infer rows through QueryBuilder<T>: include types survive useAll.
import type { QueryBuilder } from "../runtime/db.js";
declare function useAllLike<T extends { id: string }>(query: QueryBuilder<T>): T[];
const hookRows = useAllLike(app.chats.where({ isPublic: true }).include({ messagesViaChat: true }));
const hookText: string | undefined = hookRows[0]?.messagesViaChat[0]?.text;
String(hookText);
