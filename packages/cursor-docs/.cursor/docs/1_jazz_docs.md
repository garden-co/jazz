---

## **CoMap Overview**
**CoMap** is a collaborative object mapping system from `jazz-tools`, mapping string keys to values.

### **1. Basic Definition**
```typescript
import { CoMap, co } from "jazz-tools";
class Person extends CoMap {
  name = co.string;
  age = co.number;
  isActive = co.boolean;
}
```

### **2. Field Types**
- **Basic:** `co.string`, `co.number`, `co.boolean`
- **Optional:** `co.optional.string`, `co.optional.number`
- **Literals (Enums):** `co.literal("draft", "published", "archived")`
- **Dates:** `co.Date`
- **Custom Encoded:**
```typescript
customField = co.encoded({
  encode: (v: string) => v.toUpperCase(),
  decode: (v: unknown) => String(v).toLowerCase()
});
```

### **3. References to Other CoMaps**
```typescript
class Comment extends CoMap {
  text = co.string;
  createdAt = co.Date;
}
class Post extends CoMap {
  title = co.string;
  content = co.string;
  mainComment = co.ref(Comment);
  pinnedComment = co.optional.ref(Comment);
}

// Loading with resolve API (0.12.0+)
const post = await Post.load(id, {
  resolve: {
    mainComment: true,
    pinnedComment: true
  }
});
```

### **4. Lists with CoList**
```typescript
import { CoList, CoMap, co } from "jazz-tools";
class Task extends CoMap {
  title = co.string;
  completed = co.boolean;
}
class TaskList extends CoList.Of(co.ref(Task)) {}
class Project extends CoMap {
  name = co.string;
  tasks = co.ref(TaskList);
}

// Loading with resolve API (0.12.0+)
const project = await Project.load(id, {
  resolve: {
    tasks: {
      $each: true
    }
  }
});
```

### **5. Validation & Custom Methods**
```typescript
class DraftPost extends CoMap {
  title = co.optional.string;
  content = co.optional.string;
  validate() {
    const errors: string[] = [];
    if (!this.title) errors.push("Title is required");
    if (!this.content) errors.push("Content is required");
    return { errors };
  }
  get summary() { return this.content?.slice(0, 100) + "..."; }
}
```

### **Real-World Examples**
- **Chat Schema**
```typescript
class Message extends CoMap { text = co.string; image = co.optional.ref(ImageDefinition); }
class Chat extends CoList.Of(co.ref(Message)) {}
```
- **Organization Schema**
```typescript
class Project extends CoMap { name = co.string; }
class ListOfProjects extends CoList.Of(co.ref(Project)) {}
class Organization extends CoMap {
  name = co.string;
  projects = co.ref(ListOfProjects);
}
```
- **Issue Tracking**
```typescript
class Issue extends CoMap {
  title = co.string;
  description = co.string;
  estimate = co.number;
  status? = co.literal("backlog", "in progress", "done");
}
class ListOfIssues extends CoList.Of(co.ref(Issue)) {}
class Project extends CoMap {
  name = co.string;
  issues = co.ref(ListOfIssues);
}
```

### **Testing Example**
```typescript
class TestMap extends CoMap {
  color = co.string;
  _height = co.number;
  birthday = co.Date;
  name? = co.string;
  nullable = co.optional.encoded<string | undefined>({
    encode: (v: string | undefined) => v || null,
    decode: (v: unknown) => (v as string) || undefined,
  });
  optionalDate = co.optional.encoded(Encoders.Date);
  get roughColor() { return this.color + "ish"; }
}
```

### **Key Takeaways**
- Extend **CoMap** for schemas.
- Use `co.ref()` for references, `co.optional` for optional fields.
- Use `CoList.Of()` for collections.
- Fields auto-sync across clients.
- Add computed properties & validation methods.

---

## **CoList Overview**
**CoList** is a collaborative array in `jazz-tools`.

### **1. Basic Definition**
```typescript
import { CoList, co } from "jazz-tools";
class ColorList extends CoList.Of(co.string) {}
class NumberList extends CoList.Of(co.number) {}
class BooleanList extends CoList.Of(co.boolean) {}
```

### **2. Lists of CoMaps**
```typescript
import { CoList, CoMap, co } from "jazz-tools";
class Task extends CoMap { title = co.string; completed = co.boolean; }
class ListOfTasks extends CoList.Of(co.ref(Task)) {}

// Loading with resolve API (0.12.0+)
const taskList = await ListOfTasks.load(id, { resolve: { $each: true } });
```

### **3. CoList Operations**
```typescript
const taskList = ListOfTasks.create([], { owner: me });
taskList.push(Task.create({ title: "New task", completed: false }, { owner: me }));
const firstTask = taskList[0];
taskList.filter(task => !task.completed);
taskList.splice(1, 1);
```

### **4. Nested Lists**
```typescript
class Comment extends CoMap { text = co.string; createdAt = co.Date; }
class ListOfComments extends CoList.Of(co.ref(Comment)) {}
class Post extends CoMap {
  title = co.string;
  content = co.string;
  comments = co.ref(ListOfComments);
}
class ListOfPosts extends CoList.Of(co.ref(Post)) {}

// Loading with resolve API (0.12.0+)
const posts = await ListOfPosts.load(id, {
  resolve: {
    $each: {
      comments: {
        $each: true
      }
    }
  }
});
```

### **Real-World Examples**
- **Chat Schema**
```typescript
class Message extends CoMap { text = co.string; image = co.optional.ref(ImageDefinition); }
class Chat extends CoList.Of(co.ref(Message)) {}
```
- **Todo App Schema**
```typescript
class Task extends CoMap { done = co.boolean; text = co.string; }
class ListOfTasks extends CoList.Of(co.ref(Task)) {}
```
- **Organization Schema**
```typescript
class Project extends CoMap { name = co.string; }
class ListOfProjects extends CoList.Of(co.ref(Project)) {}
class Organization extends CoMap {
  name = co.string;
  projects = co.ref(ListOfProjects);
}
```

### **5. Advanced Features**
```typescript
class TaskList extends CoList.Of(co.ref(Task)) {
  getCompletedTasks() { return this.filter(task => task.completed); }
  getPendingTasks() { return this.filter(task => !task.completed); }
}
```

### **Key Takeaways**
- `CoList.Of()` for list definitions.
- `co.ref()` for CoMap references.
- Acts like arrays with real-time sync.
- Supports custom methods & nested lists.

---

## **CoFeed Overview**
**CoFeed** is an append-only event stream, ideal for time-ordered data.

### **1. Basic Definition**
```typescript
import { CoFeed, co } from "jazz-tools";
class ActivityFeed extends CoFeed.Of(co.string) {}
class MetricsFeed extends CoFeed.Of(co.number) {}
```

### **2. Feeds with Complex Types**
```typescript
interface LogEvent {
  timestamp: number;
  level: "info" | "warn" | "error";
  message: string;
}
class LogFeed extends CoFeed.Of(co.json<LogEvent>()) {}
```

### **3. Pet Reactions Example**
```typescript
export const ReactionTypes = ["aww","love","haha","wow","tiny","chonkers"] as const;
export class PetReactions extends CoFeed.Of(co.json<ReactionType>()) {}
```

### **4. Working with CoFeeds**
```typescript
const reactions = PetReactions.create({ owner: me });
reactions.post("love");
reactions.subscribe(feedId, me, {}, (feed) => console.log(feed.latest()));
```

### **5. Common Use Cases**
- **Activity Streams**
```typescript
class ActivityStream extends CoFeed.Of(co.json<{ type:"comment"|"like"|"share";userId:string;timestamp:number;}>) {}
```
- **Chat Messages**
```typescript
class ChatFeed extends CoFeed.Of(co.json<{ type:"message"|"join"|"leave";userId:string; content?:string;timestamp:number;}>) {}
```
- **Audit Logs**
```typescript
class AuditLog extends CoFeed.Of(co.json<{ action:string; user:string; details:Record<string,unknown>;timestamp:number;}>) {}
```

### **6. Differences: CoFeed vs. CoList**
| Feature  | CoFeed (append-only) | CoList (mutable) |
|----------|----------------------|------------------|
| Order    | Time-ordered        | Arbitrary        |
| Use Case | Logs, streams       | Collections      |

### **Key Takeaways**
- **CoFeed**: event logs, activity streams, append-only.
- **CoList**: modifiable lists.
- Real-time updates, easy to subscribe.

---

## **SchemaUnion Overview**
**SchemaUnion** handles runtime-discriminated union types of `CoMap` instances.

### **1. Basic Definition**
```typescript
import { SchemaUnion, CoMap, co } from "jazz-tools";
class BaseShape extends CoMap { type = co.string; }
class Circle extends BaseShape { type = co.literal("circle"); radius = co.number; }
class Rectangle extends BaseShape { type = co.literal("rectangle"); width = co.number; height = co.number; }
const Shape = SchemaUnion.Of<BaseShape>((raw) => {
  switch (raw.get("type")) {
    case "circle": return Circle;
    case "rectangle": return Rectangle;
    default: throw new Error("Unknown shape");
  }
});
```

### **2. Nested Discriminators**
```typescript
class BaseButton extends CoMap { type = co.literal("button"); variant = co.string; }
class PrimaryButton extends BaseButton { variant = co.literal("primary"); label = co.string; size = co.literal("small","medium","large"); }
class SecondaryButton extends BaseButton { variant = co.literal("secondary"); label = co.string; outline = co.boolean; }
const Button = SchemaUnion.Of<BaseButton>((raw) => {
  switch (raw.get("variant")) {
    case "primary": return PrimaryButton;
    case "secondary": return SecondaryButton;
    default: throw new Error("Unknown variant");
  }
});
```

### **3. Using SchemaUnion with CoLists**
```typescript
class BaseWidget extends CoMap { type = co.string; }
class ButtonWidget extends BaseWidget { type = co.literal("button"); label = co.string; }
class SliderWidget extends BaseWidget { type = co.literal("slider"); min = co.number; max = co.number; }
const Widget = SchemaUnion.Of<BaseWidget>((raw) => {
  switch (raw.get("type")) {
    case "button": return ButtonWidget;
    case "slider": return SliderWidget;
    default: throw new Error("Unknown widget");
  }
});
class WidgetList extends CoList.Of(co.ref(Widget)) {}
```

### **4. Working with SchemaUnion Instances**
```typescript
const button = ButtonWidget.create({ type:"button", label:"Click me" }, { owner: me });
const widget = await loadCoValue(Widget, widgetId, me, {});
if (widget instanceof ButtonWidget) console.log(widget.label);
if (widget instanceof SliderWidget) console.log(widget.min, widget.max);
```

### **5. Validation Example**
```typescript
class BaseFormField extends CoMap {
  type = co.string;
  label = co.string;
  required = co.boolean;
}
class TextField extends BaseFormField {
  type = co.literal("text");
  minLength = co.optional.number;
  maxLength = co.optional.number;
  validate(value: string){/* ... */}
}
class NumberField extends BaseFormField {
  type = co.literal("number");
  min = co.optional.number;
  max = co.optional.number;
  validate(value: number){/* ... */}
}
const FormField = SchemaUnion.Of<BaseFormField>((raw) => {
  switch (raw.get("type")) {
    case "text": return TextField;
    case "number": return NumberField;
    default: throw new Error("Unknown type");
  }
});
```

### **Key Takeaways**
- **SchemaUnion** = polymorphic CoMaps.
- Use `co.literal()` for discriminators.
- `instanceof` for type-narrowing.
- Great for complex forms & dynamic components.

---

## **Groups, Accounts, Owners, Roles & Permissions in Jazz**

### **1. Ownership & Groups**
Every `CoValue` has an owner (an `Account` or `Group`):
```typescript
import { Account, Group, CoMap, co } from "jazz-tools";
const privateDoc = Document.create({ title:"Private" }, { owner: me });
const group = Group.create({ owner: me });
const sharedDoc = Document.create({ title:"Shared" }, { owner: group });
```

### **2. Roles & Permissions**
Built-in roles: `"admin"`, `"writer"`, `"reader"`, `"readerInvite"`, `"writerInvite"`.
```typescript
group.addMember(bob, "writer");
group.addMember(alice, "reader");
group.addMember("everyone","reader");
```

### **3. Organizations & Memberships**
```typescript
import { Account, CoMap, CoList, Group, co } from "jazz-tools";
class Project extends CoMap { name = co.string; description = co.string; }
class Organization extends CoMap {
  name = co.string;
  projects = co.ref(CoList.Of(co.ref(Project)));
  static create(name: string, owner: Account) {
    const group = Group.create({ owner });
    return super.create({ name, projects: CoList.Of(co.ref(Project)).create([], { owner: group }) }, { owner: group });
  }
  addMember(account: Account, role: "admin"|"writer"|"reader") {
    this._owner.castAs(Group).addMember(account, role);
  }
}
```

### **4. Account Root & Migration Pattern**
```typescript
class TodoAccountRoot extends CoMap {
  projects = co.ref(ListOfProjects);
}
export class UserProfile extends Profile { someProperty = co.string; }
class TodoAccount extends Account {
  root = co.ref(TodoAccountRoot);
  profile = co.ref(UserProfile);
  migrate() {
    if (!this._refs.root) {
      this.root = TodoAccountRoot.create({ projects: ListOfProjects.create([], { owner: this }) }, { owner: this });
    }
  }
}
```

### **5. Public Sharing Example**
```typescript
class SharedFile extends CoMap {
  name = co.string;
  file = co.ref(FileStream);
  createdAt = co.Date;
  size = co.number;
}
class FileShareAccountRoot extends CoMap {
  type = co.string;
  sharedFiles = co.ref(ListOfSharedFiles);
  publicGroup = co.ref(Group);
}
export class UserProfile extends Profile { someProperty = co.string; }
class FileShareAccount extends Account {
  root = co.ref(FileShareAccountRoot);
  profile = co.ref(UserProfile);
  async migrate() {
    await this._refs.root?.load();
    if (!this.root || this.root.type !== "file-share-account") {
      const publicGroup = Group.create({ owner: this });
      publicGroup.addMember("everyone","reader");
      this.root = FileShareAccountRoot.create({
        type:"file-share-account",
        sharedFiles: ListOfSharedFiles.create([], { owner: publicGroup }),
        publicGroup
      }, { owner: this });
    }
  }
}
```

### **6. Group Extensions**
```typescript
const parentGroup = Group.create({ owner: me });
parentGroup.addMember(bob, "reader");
const childGroup = Group.create({ owner: me });
childGroup.extend(parentGroup);
const doc = Document.create({ title:"Inherited Access" }, { owner: childGroup });
```

### **7. Checking Permissions**
```typescript
const group = document._owner.castAs(Group);
const myRole = group.myRole();
const hasWriteAccess = myRole === "admin" || myRole === "writer";
```

### **8. Invitation Pattern**
```typescript
class TeamInvite extends CoMap {
  email = co.string;
  role = co.literal("admin","writer","reader");
  accepted = co.boolean;
}
class Team extends CoMap {
  invites = co.ref(CoList.Of(co.ref(TeamInvite)));
  async inviteMember(email: string, role: "admin"|"writer"|"reader") {
    const group = this._owner.castAs(Group);
    const invite = TeamInvite.create({ email, role, accepted:false }, { owner: group });
    this.invites.push(invite);
    group.addMember(email, (role+"Invite") as const);
  }
  acceptInvite(account: Account) {
    const group = this._owner.castAs(Group);
    const invite = this.invites.find(i => i.email === account.email);
    if (invite) {
      invite.accepted = true;
      group.addMember(account, invite.role);
    }
  }
}
```

### **Key Takeaways**
- Every `CoValue` has an owner (Account or Group).
- Groups enable sharing/role-based access (`"admin"`, `"writer"`, `"reader"`, etc.).
- Groups can inherit permissions.
- Use account roots for private per-user data.
- Public sharing via `"everyone"` role.
- Invites allow controlled membership.

---

## **Inbox Pattern in Jazz**
Enables message exchange between accounts using `CoMap`, `CoList`, `Group`.

### **1. Basic Inbox Setup**
```typescript
import { CoMap, co, Group } from "jazz-tools";
class Message extends CoMap {
  text = co.string;
  createdAt = co.Date;
  read = co.boolean;
}
class ChatInbox extends CoMap {
  messages = co.ref(CoList.Of(co.ref(Message)));
  lastReadAt = co.Date;
}
```

### **2. Sending Messages**
```typescript
async function sendMessage(sender: Account, receiverId: ID<Account>, text: string) {
  const message = Message.create(
    { text, createdAt:new Date(), read:false },
    { owner: Group.create({ owner: sender }) }
  );
  const inboxSender = await InboxSender.load(receiverId, sender);
  inboxSender.sendMessage(message);
}
```

### **3. Receiving Messages**
```typescript
async function setupInbox(receiver: Account) {
  const inbox = await Inbox.load(receiver);
  return inbox.subscribe(Message,(message,senderId)=>console.log("New:",message.text));
}
```

### **4. Chat Application Example**
```typescript
class ChatMessage extends CoMap { text = co.string; createdAt=co.Date; read=co.boolean; }
class ChatThread extends CoMap {
  participants = co.json<string[]>();
  messages = co.ref(CoList.Of(co.ref(ChatMessage)));
  lastReadAt = co.optional.Date;
}
class ChatRoot extends CoMap {
  threads = co.ref(CoList.Of(co.ref(ChatThread)));
  inbox = co.ref(Inbox);
}
export class UserProfile extends Profile { someProperty=co.string; }
class ChatAccount extends Account {
  root = co.ref(ChatRoot);
  profile = co.ref(UserProfile);
  async migrate() {
    if(!this._refs.root) {
      const group = Group.create({ owner:this });
      this.root = ChatRoot.create({
        threads: CoList.Of(co.ref(ChatThread)).create([], { owner: group }),
        inbox: await Inbox.create(this)
      },{ owner:this });
    }
  }
  async sendMessage(to: ID<Account>, text:string) {
    const message = ChatMessage.create({ text, createdAt:new Date(), read:false },
      { owner: Group.create({ owner:this }) });
    const inboxSender=await InboxSender.load(to,this);
    inboxSender.sendMessage(message);
  }
  async setupInboxListener() {
    const inbox = await Inbox.load(this);
    return inbox.subscribe(ChatMessage, async (message, senderId) => {
      const thread = await this.findOrCreateThread(senderId);
      thread.messages.push(message);
    });
  }
}
```

### **5. Testing the Inbox Pattern**
```typescript
describe("Inbox", () => {
  it("should allow message exchange", async () => {
    const { clientAccount:sender, serverAccount:receiver } = await setupTwoNodes();
    const receiverInbox = await Inbox.load(receiver);
    const message = Message.create({ text:"Hello" },{ owner:Group.create({ owner:sender }) });
    const inboxSender = await InboxSender.load(receiver.id, sender);
    inboxSender.sendMessage(message);
    const receivedMessages: Message[] = [];
    let senderAccountID: unknown;
    const unsubscribe = receiverInbox.subscribe(Message, (msg, id) => {
      senderAccountID=id; receivedMessages.push(msg);
    });
    await waitFor(() => receivedMessages.length===1);
    expect(receivedMessages[0]?.text).toBe("Hello");
    expect(senderAccountID).toBe(sender.id);
    unsubscribe();
  });
});
```

### **6. Message Status Tracking**
```typescript
class MessageStatus extends CoMap {
  messageId = co.string;
  delivered = co.boolean;
  read = co.boolean;
  readAt = co.optional.Date;
}
class EnhancedMessage extends CoMap {
  text = co.string;
  createdAt = co.Date;
  status = co.ref(MessageStatus);
}
async function sendMessageWithStatus(sender: Account, receiverId: ID<Account>, text:string) {
  const group = Group.create({ owner:sender });
  const status = MessageStatus.create({ messageId:crypto.randomUUID(), delivered:false, read:false }, { owner:group });
  const message = EnhancedMessage.create({ text, createdAt:new Date(), status }, { owner:group });
  const inboxSender = await InboxSender.load(receiverId,sender);
  inboxSender.sendMessage(message);
  return message;
}
```

### **Key Takeaways**
- Messages owned by a `Group` from the sender.
- Use `InboxSender.load()` to send, `Inbox.load()` to receive.
- Subscribe for real-time updates.
- Append status tracking as needed.

---

## **Invite Pattern in Jazz**
Sharing access to `CoValues` with other users via invites.

### **1. Creating & Handling Invites**
```typescript
import { CoMap, Group, co, createInviteLink } from "jazz-tools";
class Project extends CoMap { name = co.string; members = co.ref(CoList.Of(co.ref(Member))); }
const group = Group.create({ owner: me });
const project = Project.create({ name:"New Project", members:CoList.Of(co.ref(Member)).create([],{owner:group}) }, { owner:group });
const readerInvite = createInviteLink(project,"reader");
const writerInvite = createInviteLink(project,"writer");
const adminInvite = createInviteLink(project,"admin");
```

### **2. Accepting Invites in UI**
```typescript
import { useAcceptInvite } from "jazz-react";
useAcceptInvite({
  invitedObjectSchema:Project,
  onAccept:(projectId)=>navigate(`/projects/${projectId}`)
});
```

### **3. Organization Example**
```typescript
class Organization extends CoMap {
  name = co.string;
  projects = co.ref(ListOfProjects);
  createInvite(role:"reader"|"writer"|"admin"){ return createInviteLink(this,role); }
}
useAcceptInvite({
  invitedObjectSchema:Organization,
  onAccept:async(orgId)=>{/* ... */}
});
```

### **4. Value Hints in Invites**
```typescript
class Team extends CoMap {
  name = co.string;
  generateInvite(role:"reader"|"writer"|"admin"){ return createInviteLink(this, role, window.location.origin, "team"); }
}
useAcceptInvite({
  invitedObjectSchema:Team,
  forValueHint:"team",
  onAccept:(teamId)=>navigate(`/teams/${teamId}`)
});
```

### **5. Testing Invites**
```typescript
describe("Invite Links", () => {
  test("generate and parse invites", async () => {
    const inviteLink = createInviteLink(group, "writer","https://example.com","myGroup");
    const parsed = parseInviteLink(inviteLink);
    expect(parsed?.valueID).toBe(group.id);
    expect(parsed?.valueHint).toBe("myGroup");
  });
  test("accept invite", async () => {
    const newAccount = await createJazzTestAccount();
    const inviteLink = createInviteLink(group, "writer");
    const result = await consumeInviteLink({ inviteURL: inviteLink, as:newAccount, invitedObjectSchema:Group });
    expect(result?.valueID).toBe(group.id);
  });
});
```

### **6. File Sharing with Invites**
```typescript
class SharedFile extends CoMap {
  name = co.string;
  sharedWith = co.ref(CoList.Of(co.ref(SharedWith)));
}
class SharedWith extends CoMap {
  email = co.string;
  role = co.literal("reader","writer");
  acceptedAt = co.optional.Date;
}
class FileShareAccount extends Account {
  async shareFile(file:SharedFile, email:string, role:"reader"|"writer") {
    const inviteLink = createInviteLink(file,role);
    file.sharedWith.push(SharedWith.create({ email, role, acceptedAt:null },{ owner:file._owner }));
    await sendInviteEmail(email, inviteLink);
  }
}
useAcceptInvite({
  invitedObjectSchema:SharedFile,
  onAccept:async(fileId)=>{
    const file = await SharedFile.load(fileId,{});
    const shareRecord = file.sharedWith.find(s=>s.email===currentUser.email);
    if(shareRecord) shareRecord.acceptedAt=new Date();
    navigate(`/files/${fileId}`);
  }
});
```

### **Key Takeaways**
- Use Groups for shared ownership.
- `createInviteLink()` generates invites.
- `useAcceptInvite()` handles acceptance.
- Value hints (`forValueHint`) differentiate invite types.

---

## **CoValue Types & Patterns in Jazz**

### **1. CoMap**
- Use for structured data with named fields.
- Example:
```typescript
class UserProfile extends CoMap {
  name = co.string;
  email = co.string;
  avatar = co.ref(FileStream);
  preferences = co.json<{ theme:string; notifications:boolean }>();
}
class TagColors extends CoMap.Record(co.string) {}
```

### **2. CoList**
- Use for ordered, real-time collaborative arrays.
```typescript
class TodoList extends CoList.Of(co.ref(TodoItem)) {}
class StringList extends CoList.Of(co.string) {}
```

### **3. CoFeed**
- Use for append-only event/log data.
```typescript
class UserActivity extends CoFeed.Of(co.json<{ type:string; timestamp:number; text?:string }>) {}
```

### **4. SchemaUnion**
- Use for polymorphic objects with a runtime discriminator.
```typescript
class BaseWidget extends CoMap { type=co.string; }
class ButtonWidget extends BaseWidget { type=co.literal("button"); label=co.string; }
const Widget=SchemaUnion.Of<BaseWidget>(raw=>raw.get("type")==="button"?ButtonWidget:null);
```

### **5. Groups & Permissions**
- Owner can be an Account or Group.
- Roles: `"admin"|"writer"|"reader"|"readerInvite"|"writerInvite"`.
```typescript
const group=Group.create({owner:me});
group.addMember("everyone","reader");
```

### **6. Accounts**
- Per-user data storage with migrations.
```typescript
class JazzAccount extends Account {
  root=co.ref(JazzAccountRoot);
  profile=co.ref(UserProfile);
  async migrate(){/*...*/}
}
```

### **7. Migrations**
- Update/initialize user data on account creation/login.

### **8. Invites**
- Role-based sharing through invite links.

### **Common Patterns**
- **Account Root Pattern**: store user's top-level data.
- **Shared Document Pattern**: CoMap for doc, CoList for collaborators, CoFeed for history.
- **Draft Pattern**: CoMap with partial fields, validation.
- **Public Sharing**: set `Group.addMember("everyone","reader")`.

### **New in 0.12.0: The Resolve API**
- Use for type-safe deep loading of nested data.
- Consistent pattern across all loading methods.
```typescript
// Simple resolve
const data = await MyClass.load(id, { resolve: true });

// Deep resolve with collections
const tasks = await ListOfTasks.load(id, {
  resolve: {
    $each: {
      assignees: {
        $each: { org: true }
      }
    }
  }
});

// The Resolved type helper
type TaskListResolved = Resolved<ListOfTasks, {
  $each: { assignee: true }
}>;

function TaskComponent({ taskList }: { taskList: TaskListResolved }) {
  // Type-safe access to resolved data
  return taskList.map(task => task.assignee);
}
```

### **Loading States in 0.12.0+**
- `undefined`: Item is loading
- `null`: Item is missing (not found or access denied)

---

## **Examples**

1. **User Profile Storage (CoMap)**
**JSON**:
```json
{
  "name":"John Doe","email":"john@example.com",
  "avatar":{"url":"...","size":"..."},
  "preferences":{"theme":"dark","notifications":true}
}
```
**Jazz**:
```typescript
class UserProfile extends CoMap {
  name = co.string;
  email = co.string;
  avatar = co.ref(FileStream);
  preferences = co.json<{theme:string;notifications:boolean}>();
}
```

2. **To-Do List (CoList)**
**JSON**:
```json
{"tasks":[{"id":1,"title":"Buy groceries","completed":false},{"id":2,"title":"Call mom","completed":true}]}
```
**Jazz**:
```typescript
class TodoItem extends CoMap { title=co.string; completed=co.boolean; }
class TodoList extends CoList.Of(co.ref(TodoItem)) {}
```

3. **Activity Feed (CoFeed)**
**JSON**:
```json
{"activities":[{"type":"login","timestamp":1700000000},{"type":"logout","timestamp":1700000500},{"type":"comment","timestamp":1700001000,"text":"Great post!"}]}
```
**Jazz**:
```typescript
class UserActivity extends CoFeed.Of(co.json<{type:string;timestamp:number;text?:string}>()) {}
```

4. **Polymorphic Widgets (SchemaUnion)**
**JSON**:
```json
{"widgets":[{"type":"button","label":"Click Me"},{"type":"slider","min":0,"max":100}]}
```
**Jazz**:
```typescript
class BaseWidget extends CoMap { type=co.string; }
class ButtonWidget extends BaseWidget { type=co.literal("button"); label=co.string; }
class SliderWidget extends BaseWidget { type=co.literal("slider"); min=co.number; max=co.number; }
const Widget=SchemaUnion.Of<BaseWidget>((raw)=>{...});
```

5. **Access Control via Groups**
**JSON**:
```json
{"group":{"owner":"user123","members":[{"id":"user456","role":"admin"},{"id":"user789","role":"writer"}]}}
```
**Jazz**:
```typescript
const group=Group.create({owner:user123});
group.addMember(user456,"admin");
group.addMember(user789,"writer");
```

6. **User Account with Root Data (Accounts)**
**JSON**:
```json
{"user":{"profile":{"name":"Jane Doe"},"documents":[{"title":"My Notes","content":"This is a note."}],"activities":[{"type":"login"}]}}
```
**Jazz**:
```typescript
class AppAccountRoot extends CoMap {
  profile=co.ref(UserProfile);
  documents=co.ref(CoList.Of(co.ref(Document)));
  activities=co.ref(UserActivity);
}
class AppAccount extends Account {
  root=co.ref(AppAccountRoot);
  profile=co.ref(UserProfile);
}
```

7. **Document Collaboration**
**JSON**:
```json
{
  "document":{
    "title":"Project Plan","content":"Detailed...","collaborators":[{"id":"user1","role":"editor"},{"id":"user2","role":"viewer"}],
    "history":[{"user":"user1","timestamp":1700000000,"change":"Edited content"}]
  }
}
```
**Jazz**:
```typescript
class Document extends CoMap {
  title=co.string;content=co.string;
  collaborators=co.ref(CoList.Of(co.ref(UserProfile)));
  history=co.ref(CoFeed.Of(co.json<{user:string;timestamp:number;change:string}>()));
}
```

8. **Draft System**
**JSON**:
```json
{"draft":{"name":"New Project","tasks":[],"valid":false,"errors":["Project name required"]}}
```
**Jazz**:
```typescript
class DraftProject extends CoMap {
  name=co.optional.string;
  tasks=co.ref(CoList.Of(co.ref(TodoItem)));
  validate(){/*...*/}
}
```

9. **Public File Sharing**
**JSON**:
```json
{"file":{"name":"Presentation.pdf","size":2048,"uploadedAt":1700000000,"sharedWith":["everyone"]}}
```
**Jazz**:
```typescript
class SharedFile extends CoMap {
  name=co.string;
  file=co.ref(FileStream);
  uploadedAt=co.Date;
}
const publicGroup=Group.create({owner:me});
publicGroup.addMember("everyone","reader");
```

10. **Invite System**
**JSON**:
```json
{"invites":[{"email":"user@example.com","role":"writer","status":"pending"}]}
```
**Jazz**:
```typescript
class Invite extends CoMap {
  email=co.string;
  role=co.literal("reader","writer","admin");
  status=co.literal("pending","accepted");
}
const inviteLink=createInviteLink(project,"writer");
useAcceptInvite({ invitedObjectSchema:Project, onAccept:(id)=>navigate(`/projects/${id}`) });
```

---

Continue: 2_jazz_schema_template.md
