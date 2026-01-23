import { co, z } from "jazz-tools";
import { createJazzTestAccount } from "jazz-tools/testing";

const me = await createJazzTestAccount({
  isCurrentActiveAccount: true,
});

// #region DefiningSchemas
// 1. CoMaps: Object-like with fixed keys
const ToDo = co.map({
  task: z.string(),
  completed: z.boolean(),
  dueDate: z.date().optional(),
});

// 2. CoRecords: Object-like with arbitrary string keys
const PhoneBook = co.record(z.string(), z.string()); // co.record(keyType, valueType)

// 3. CoLists: Array-like ordered list
const ToDoList = co.list(ToDo); // co.list(itemType)

// 4. CoFeeds: Array-like append-only list
const Message = co.map({ text: z.string() });
const ChatMessages = co.feed(Message); // co.feed(itemType)

// 5. CoPlainTexts/CoRichTexts: String-like
const Description = co.plainText(); // or co.richText();

// 6. FileStreams: Blob-like
const UploadedPDF = co.fileStream();

// 7. ImageDefinitions: Blob-like
const UploadedImage = co.image();

// 8. CoVectors: Array-like list of numbers/Float32Array
const Embedding = co.vector(384); // co.vector(dimensions)

// 9. DiscriminatedUnions: Union of different types of items
const ThisSchema = co.map({
  type: z.literal("this"),
  thisProperty: z.string(),
});
const ThatSchema = co.map({
  type: z.literal("that"),
  thatProperty: z.string(),
});
const MyThisOrThat = co.discriminatedUnion("type", [ThisSchema, ThatSchema]); // co.discriminatedUnion(discriminatorKey, arrayOfSchemas)
// #endregion

// #region InlineCreation
const Task = co.map({
  title: z.string(),
  completed: z.boolean(),
});

const TaskList = co.list(Task);
const taskList = TaskList.create([
  { title: "Task 1", completed: false }, // These will create new Task CoValues
  { title: "Task 2", completed: false }, // both will be inserted into the TaskList
]);
// #endregion

// #region Permissions
const group = co.group().create();
const task = Task.create(
  { title: "Buy milk", completed: false },
  { owner: group },
);
// #endregion

const user = co
  .map({
    name: z.string(),
  })
  .create({ name: "Alice" });

const phoneBook = PhoneBook.create({});
// #region CoMapReading
// CoMap: Access fixed keys
console.log(user.name); // "Alice"

// CoRecord: Access arbitrary keys
const phone = phoneBook["Jenny"];

// Iteration works as with a TypeScript object
for (const [name, number] of Object.entries(phoneBook)) {
  console.log(name, number);
}
// #endregion

// #region CoListReading
const firstTask = taskList[0];
const length = taskList.length;

// Iteration works as with a TypeScript array
taskList.map((task) => console.log(task.title));
for (const task of taskList) {
  // Do something
}
// #endregion

const description = co.plainText().create("Test");
// #region CoTextReading
// String operations
const summary = description.substring(0, 100);
// #endregion

const chatMessages = ChatMessages.create([]);

// Yeah... the type of this is down in cojson and I don't see it exposed higher up, so I'm just any-typing this to avoid making bigger changes.
const thisSessionId = "" as any;
const accountId = "";
// #region CoFeedReading
// Get the feed for a specific session (e.g. this browser tab)
const thisSessionsFeed = chatMessages.perSession[thisSessionId]; // or .inCurrentSession as shorthand
const latestMessageFromThisSession = thisSessionsFeed.value;
const allMessagesFromThisSession = thisSessionsFeed.all;

// Get the feed for a specific account
const accountFeed = chatMessages.perAccount[accountId];
const latestMessageFromThisAccount = accountFeed.value;
const allMessagesFromThisAccount = accountFeed.all;

// Get the feed for my account
const myFeed = chatMessages.byMe; // shorthand for chatMessages.perAccount[myAccountId]
const latestMessageFromMyAccount = myFeed?.value;
const allMessagesFromMyAccount = myFeed?.all;

// Iterate over all entries in a CoFeed
for (const userId of Object.keys(chatMessages.perAccount)) {
  const accountFeed = chatMessages.perAccount[userId];
  for (const entry of accountFeed.all) {
    if (entry.value.$isLoaded) {
      console.log(entry.value);
    }
  }
}
// #endregion

const fileStream = co.fileStream().create();
// #region FileStreamReading
// Get raw data chunks and metadata.
// Optionally pass { allowUnfinished: true } to get chunks of a FileStream which is not yet fully synced.
const fileData = fileStream.getChunks({ allowUnfinished: true });

// Convert to a Blob for use in a <a> tag or <iframe>
const fileBlob = fileStream.toBlob();
const fileUrl = fileBlob && URL.createObjectURL(fileBlob);
// #endregion

const productImage = "";
// #region ImageDefinitionReading
// Imperative usage: Access the highest available resolution
import { loadImageBySize } from "jazz-tools/media";

// Not guaranteed to exist if no variant exists that fulfills your constraints
const imageDef = await loadImageBySize(productImage, 300, 400); // Takes either an ImageDefinition or an ID, and returns a FileStream.
// #endregion

const myImg = document.createElement("img");

// #region ImageDefinitionBlob
const blob = imageDef && imageDef.image.toBlob();
const url = blob && URL.createObjectURL(blob); // Don't forget to clean this up when you're done!
myImg.src = url ?? "";
// #endregion

const myEmbedding = Embedding.create([]);
const targetVector = Embedding.create([]);
// #region CoVectorReading
// Calculate similarity between two vectors
const similarity = myEmbedding.$jazz.cosineSimilarity(targetVector);
// #endregion

// This isn't a true discriminated union
const item: {
  type: "task" | "note";
  title: string;
  content: string;
} = {
  type: "task",
  title: "",
  content: "",
};
// #region DiscriminatedUnionReading
// Use the discriminator to check the type
if (item.type === "task") {
  console.log(item.title);
} else if (item.type === "note") {
  console.log(item.content);
}
// #endregion

const todo = ToDo.create({ task: "Try Jazz", completed: false });
// #region UpdatingCoMaps
// Set or update a property
todo.$jazz.set("task", "Try out Jazz");

// Delete an optional property
todo.$jazz.delete("dueDate");

// Update multiple properties at once
todo.$jazz.applyDiff({
  task: "Apply a diff to update a task",
  completed: true,
});
// #endregion

const tasks = ToDoList.create([]);
const newTask = ToDo.create({ task: "abc", completed: false });
const importantTask = newTask;
const replacementTask = newTask;
const task2 = newTask;
const task3 = newTask;
// #region UpdatingCoLists
// Add items
tasks.$jazz.push(newTask);
tasks.$jazz.unshift(importantTask);

// Remove items
const removed = tasks.$jazz.remove(0); // Remove by index, returns removed items
tasks.$jazz.remove((task) => task.completed); // Remove by predicate
const lastTask = tasks.$jazz.pop(); // Remove and return last item
const task1 = tasks.$jazz.shift(); // Remove and return first item

// Retain only matching items
tasks.$jazz.retain((task) => !task.completed); // Keep only incomplete tasks

// Replace/Move
tasks.$jazz.splice(1, 1, replacementTask);

if (!task1?.$isLoaded) throw new Error(); // [!code hide]
// Efficiently update to match another list
tasks.$jazz.applyDiff([task1, task2, task3]); // Updates list to match exactly
// #endregion

// #region UpdatingCoTexts
const message = co.plainText().create("Hello world!"); // Hello world
message.insertAfter(4, ","); // Hello, world!
message.insertBefore(7, "everybody in the "); // Hello, everybody in the world!
message.deleteRange({ from: 16, to: 29 }); // Hello, everybody!

// Update efficiently
message.$jazz.applyDiff("Hello, my Jazzy friends!"); // 'Hello, ' has not changed and will not be updated
// #endregion

const newMessage = Message.create({ text: "test" });
const feed = ChatMessages.create([]);
// #region UpdatingCoFeeds
feed.$jazz.push(newMessage);
// #endregion

const chunks = new Uint8Array();
// #region UpdatingFileStreams
const myUploadedPDF = UploadedPDF.create(); // Advanced usage: manual chunk streaming
myUploadedPDF.start({ mimeType: "application/pdf" });
myUploadedPDF.push(chunks); // Uint8Array
myUploadedPDF.end();
// #endregion

const some800x600Blob = new Blob();
const myUploadedImage = UploadedImage.create({
  original: co.fileStream().create(),
  originalSize: [1, 1],
  progressive: false,
});
// #region UpdatingImageDefinitions
const w = 800;
const h = 600;
const imageFile = await co.fileStream().createFromBlob(some800x600Blob);
myUploadedImage.$jazz.set(`${w}x${h}`, imageFile);
// #endregion

// #region UpdatingDiscriminatedUnions
const myLoadedThisOrThat = await MyThisOrThat.load("co_z...");

if (myLoadedThisOrThat.$isLoaded && myLoadedThisOrThat.type === "this") {
  myLoadedThisOrThat.$jazz.set("thisProperty", "Only available on 'this'!");
} else if (myLoadedThisOrThat.$isLoaded && myLoadedThisOrThat.type === "that") {
  myLoadedThisOrThat.$jazz.set("thatProperty", "Only available on 'that'!");
}
// #endregion

const shallowProfile = co
  .map({
    avatar: co.fileStream(),
  })
  .create({
    avatar: co.fileStream().create(),
  });
// #region EnsureLoaded
const profile = await shallowProfile.$jazz.ensureLoaded({
  resolve: {
    avatar: true,
  },
});
console.log(profile.avatar); // Safe to access
// #endregion

const Pet = co.map({
  name: z.string(),
});
const person = co
  .map({
    pet: Pet,
  })
  .create({
    pet: {
      name: "Fido",
    },
  });

// #region Refs
// Check if a reference exists without loading it
if (person.$jazz.refs.pet) {
  console.log("Pet ID:", person.$jazz.refs.pet.id);
}
// #endregion

// #region Subscribe
const unsub = person.$jazz.subscribe((updatedPerson) => {
  console.log("Person updated:", updatedPerson);
});
// #endregion

// HERE LIE CANARIES
// These aren't intended to be displayed anywhere: they're referenced inline in the API Reference document, but they will break the homepage build if we change our APIs. If you are encountering a build error due to anything below this page, please be sure to update docs/api-reference.mdx

// CoValue metadata
todo.$jazz.id;
todo.$jazz.owner;
todo.$jazz.createdAt;
todo.$jazz.createdBy;
todo.$jazz.lastUpdatedAt;

// CoValue loading state
todo.$jazz.loadingState;

// CoValue reactivity
todo.$jazz.waitForSync();

// CoValue version control
todo.$jazz.isBranched;
todo.$jazz.branchName;
todo.$jazz.unstable_merge();

// .has()
todo.$jazz.has("task");

// CoValue JSON representation
todo.toJSON();

// CoFeed.inCurrentSession
chatMessages.inCurrentSession;
chatMessages.inCurrentSession?.value;
chatMessages.inCurrentSession?.all;
