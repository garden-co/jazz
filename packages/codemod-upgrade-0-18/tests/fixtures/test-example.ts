// @ts-nocheck

// Test file demonstrating old Jazz API patterns that will be migrated
// This file shows examples of what the codemod will transform

import { Account, co, Group, z } from "jazz-tools";

const Message = co.map({
  text: z.string(),
});
const Chat = co.list(Message);

const Union = co.discriminatedUnion("type", [
  co.map({
    type: z.literal("a"),
    name: z.string(),
  }),
  co.map({
    type: z.literal("b"),
    name: z.string(),
  }),
]);
type Union = co.loaded<typeof Union>;

// Example 1: Property access patterns
function examplePropertyAccess() {
  const chat = Chat.create([], Group.create());
  const chatId = chat.id; // Will become: chat.$jazz.id
  const owner = chat._owner.castAs(Group); // Will become: chat.$jazz.owner
  const raw = chat._raw; // Will become: chat.$jazz.raw
  const refs = chat._refs.messages; // Will become: chat.$jazz.refs.messages
  const edits = chat._edits.title; // Will become: chat.$jazz.getEdits().title
  const type = chat._type; // Will become: chat.$type$
  const createdAt = chat._createdAt; // Will become: chat.$jazz.createdAt
  const lastUpdatedAt = chat._lastUpdatedAt; // Will become: chat.$jazz.lastUpdatedAt

  // Should keep conditionals
  const id = chat?.id;

  return {
    chatId,
    owner,
    raw,
    refs,
    edits,
    type,
    createdAt,
    lastUpdatedAt,
  };
}

function invalidSimilarCode() {
  const arr = [1, 2, 3];
  arr.push(4);
  arr.splice(0, 1);
  arr[0] = 5;
  delete arr[0];

  const obj: Record<string, string> = {
    name: "John",
    age: "30",
    city: "New York",
    castAs() {
      return this;
    },
  };
  obj.name = "Jane";
  delete obj.age;
  obj.castAs(Group);
}

function invalidSimilarCode2(args: { id: string; message: Message }) {
  return args.id;
}

// Example 2: Property assignments
function examplePropertyAssignments(union: Union) {
  const me = Account.getMe();

  if (me.profile) {
    me.profile.name = "New Name"; // Will become: me.profile.$jazz.applyDiff({"name": "New Name"})
    me.profile.avatar = undefined; // Will become: me.profile.$jazz.delete("avatar")
  }

  const chat = Message.create({ text: "Hello" }, Group.create());
  chat.text = "My Chat";

  union.name = "John"; // Will become: union.$jazz.applyDiff({"name": "John"}) because unions do not support set
}

// Example 3: Array operations
function exampleArrayOperations() {
  const chat = Chat.create([], Group.create());
  const message = Message.create({ text: "Hello" }, chat._owner);

  chat.push(message); // Will become: chat.messages.$jazz.push(message)
  chat.splice(0, 1); // Will become: chat.messages.$jazz.splice(0, 1)
  chat.unshift(message); // Will become: chat.messages.$jazz.unshift(message)
  chat.shift(); // Will become: chat.messages.$jazz.shift()
  chat.pop(); // Will become: chat.messages.$jazz.pop()
}

// Example 4: Method calls
async function exampleMethodCalls() {
  const chat = Chat.create([], Group.create());

  await chat.waitForSync(); // Will become: await chat.$jazz.waitForSync()
  await chat.waitForAllCoValuesSync(); // Will become: await chat.$jazz.waitForAllCoValuesSync()

  const unsubscribe = chat.subscribe({}, (updatedChat) => {
    console.log("Chat updated:", updatedChat.id);
  }); // Will become: chat.$jazz.subscribe({}, callback)

  const loadedChat = await chat.ensureLoaded({
    resolve: { messages: true },
  }); // Will become: chat.$jazz.ensureLoaded(options)
}

// Example 5: Rich text operations
function exampleRichTextOperations() {
  const richText = co.richText().create("Initial text", Group.create());

  richText.applyDiff("Updated text"); // Will become: richText.$jazz.applyDiff("Updated text")
}

// Example 6: Complex property chains
function exampleComplexChains() {
  const chat = Chat.create([], Group.create());
  const message = chat[0];

  if (message && message._edits?.text?.by?.profile?.name) {
    const authorName = message._edits.text.by.profile.name;
    // Will become: message.$jazz.getEdits()?.text?.by?.profile?.name
  }

  const messageOwner = message?._owner; // Will become: message?.$jazz.owner
  const messageRefs = message?._refs?.text; // Will become: message?.$jazz.refs?.text
}

// Example 7: Conditional expressions
function exampleConditionals() {
  const chat = Chat.create([], Group.create());

  if (chat._edits?.title) {
    // Will become: chat.$jazz.getEdits()?.title
    console.log("Chat has title edits");
  }

  const hasOwner = chat._owner ? true : false; // Will become: chat.$jazz.owner ? true : false
}

// Example 8: Delete operations
function exampleDeleteOperations() {
  const me = Account.getMe();

  delete me.profile.avatar; // Will become: me.profile.$jazz.delete("avatar")
  delete me.profile.bio; // Will become: me.profile.$jazz.delete("bio")
}

// Example 9: Type checking
function exampleTypeChecking() {
  const obj = Chat.create([], Group.create());

  if (obj._type === "Group") {
    // Will become: obj.$type$ === 'Group'
    console.log("This is a Group");
  }

  if (obj._type === "Chat") {
    // Will become: obj.$type$ === 'Chat'
    console.log("This is a Chat");
  }
}

// Example 10: Timestamp access
function exampleTimestamps() {
  const message = Message.create({ text: "Hello" }, Group.create());

  const created = message._createdAt; // Will become: message.$jazz.createdAt
  const lastEdit = message._edits?.text?.madeAt; // Will become: message.$jazz.getEdits()?.text?.madeAt
}

// Example 11: Destructuring assignments
function exampleDestructuring() {
  const chat = Chat.create([], Group.create());
  const message = Message.create({ text: "Hello" }, Group.create());

  // Simple id destructuring
  const { id } = chat; // Will become: const { id } = chat.$jazz

  // Multiple properties from $jazz
  const { id: chatId, _owner, _raw, _createdAt } = chat; // Will become: const { id: chatId, owner: _owner, raw: _raw, createdAt: _createdAt } = chat.$jazz

  // Type property destructuring
  const { _type } = message; // Will become: const { $type$: _type } = message

  // Mixed properties (should split into multiple statements)
  const {
    id: messageId,
    _type: messageType,
    _owner: messageOwner,
    _raw: messageRaw,
  } = message; // Will split into separate statements

  // Regular properties should not be transformed
  const { text } = message; // Should remain unchanged

  return {
    id,
    chatId,
    _owner,
    _raw,
    _createdAt,
    _type,
    messageId,
    messageType,
    messageOwner,
    messageRaw,
    text,
  };
}

export {
  examplePropertyAccess,
  examplePropertyAssignments,
  exampleArrayOperations,
  exampleMethodCalls,
  exampleRichTextOperations,
  exampleComplexChains,
  exampleConditionals,
  exampleDeleteOperations,
  exampleTypeChecking,
  exampleTimestamps,
  exampleDestructuring,
  invalidSimilarCode,
};
