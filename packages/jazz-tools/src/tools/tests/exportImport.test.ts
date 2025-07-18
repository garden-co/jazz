import { cojsonInternals } from "cojson";
import { assert, beforeEach, describe, expect, test } from "vitest";
import { exportCoValue, importContentPieces } from "../coValues/interfaces.js";
import { Account, CoPlainText, Group, co, z } from "../exports.js";
import {
  createJazzTestAccount,
  createJazzTestGuest,
  setupJazzTestSync,
} from "../testing.js";

cojsonInternals.CO_VALUE_LOADING_CONFIG.RETRY_DELAY = 10;

beforeEach(async () => {
  await setupJazzTestSync();
  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

describe("exportCoValue", () => {
  test("exports a simple CoMap", async () => {
    const Person = co.map({
      name: z.string(),
      age: z.number(),
    });

    const group = Group.create();
    const person = Person.create({ name: "John", age: 30 }, group);
    group.addMember("everyone", "reader");

    const alice = await createJazzTestAccount();

    const exported = await exportCoValue(Person, person.id, {
      loadAs: alice,
    });

    expect(exported).not.toBeNull();
    expect(exported).toBeInstanceOf(Array);
    expect(exported!.length).toBeGreaterThan(0);

    // Verify the exported content contains the person data
    const hasPersonContent = exported!.some((piece) => piece.id === person.id);
    expect(hasPersonContent).toBe(true);
  });

  test("exports a CoMap with nested references", async () => {
    const Address = co.map({
      street: z.string(),
      city: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      address: Address,
    });

    const group = Group.create();
    const address = Address.create(
      { street: "123 Main St", city: "New York" },
      group,
    );
    const person = Person.create({ name: "John", address }, group);
    group.addMember("everyone", "reader");

    const alice = await createJazzTestAccount();

    const exported = await exportCoValue(Person, person.id, {
      resolve: { address: true },
      loadAs: alice,
    });

    expect(exported).not.toBeNull();
    expect(exported).toBeInstanceOf(Array);
    expect(exported!.length).toBeGreaterThan(0);

    // Verify both person and address content are exported
    const personContent = exported!.filter((piece) => piece.id === person.id);
    const addressContent = exported!.filter((piece) => piece.id === address.id);

    expect(personContent.length).toBeGreaterThan(0);
    expect(addressContent.length).toBeGreaterThan(0);
  });

  test("exports a CoList", async () => {
    const TodoList = co.list(z.string());

    const group = Group.create();
    const todos = TodoList.create([], group);
    todos.push("Buy groceries");
    todos.push("Walk the dog");
    group.addMember("everyone", "reader");

    const alice = await createJazzTestAccount();

    const exported = await exportCoValue(TodoList, todos.id, {
      loadAs: alice,
    });

    expect(exported).not.toBeNull();
    expect(exported).toBeInstanceOf(Array);
    expect(exported!.length).toBeGreaterThan(0);

    const hasTodoContent = exported!.some((piece) => piece.id === todos.id);
    expect(hasTodoContent).toBe(true);
  });

  test("exports a CoStream", async () => {
    const ChatStream = co.feed(z.string());

    const group = Group.create();
    const chat = ChatStream.create([], group);
    chat.push("Hello");
    chat.push("World");
    group.addMember("everyone", "reader");

    const alice = await createJazzTestAccount();

    const exported = await exportCoValue(ChatStream, chat.id, {
      loadAs: alice,
    });

    expect(exported).not.toBeNull();
    expect(exported).toBeInstanceOf(Array);
    expect(exported!.length).toBeGreaterThan(0);

    const hasChatContent = exported!.some((piece) => piece.id === chat.id);
    expect(hasChatContent).toBe(true);
  });

  test("returns null for unauthorized CoValue", async () => {
    const Person = co.map({
      name: z.string(),
    });

    const group = Group.create();
    const person = Person.create({ name: "John" }, group);
    // Don't add any members, so it's private

    const alice = await createJazzTestAccount();

    const exported = await exportCoValue(Person, person.id, {
      loadAs: alice,
    });

    expect(exported).toBeNull();
  });

  test("exports with custom resolve options", async () => {
    const Address = co.map({
      street: z.string(),
      city: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      address: Address,
    });

    const group = Group.create();
    const address = Address.create(
      { street: "123 Main St", city: "New York" },
      group,
    );
    const person = Person.create({ name: "John", address }, group);
    group.addMember("everyone", "reader");

    const alice = await createJazzTestAccount();

    // Export without resolving nested references
    const exportedWithoutResolve = await exportCoValue(Person, person.id, {
      resolve: { address: false },
      loadAs: alice,
    });

    // Export with resolving nested references
    const exportedWithResolve = await exportCoValue(Person, person.id, {
      resolve: { address: true },
      loadAs: alice,
    });

    expect(exportedWithoutResolve).not.toBeNull();
    expect(exportedWithResolve).not.toBeNull();

    // The version with resolve should have more content pieces
    expect(exportedWithResolve!.length).toBeGreaterThanOrEqual(
      exportedWithoutResolve!.length,
    );
  });

  test("exports should handle errors on child covalues gracefully", async () => {
    const Address = co.map({
      street: co.plainText(),
      city: co.plainText(),
    });

    const Person = co.map({
      name: z.string(),
      address: Address,
    });

    const group = Group.create();
    const address = Address.create(
      {
        street: CoPlainText.create("123 Main St"),
        city: CoPlainText.create("New York"),
      },
      group,
    );
    const person = Person.create({ name: "John", address }, group);

    // Only add the person to the group, not the address
    // This makes the address unauthorized for other accounts
    group.addMember("everyone", "reader");

    const alice = await createJazzTestAccount();

    // Export from alice's perspective with resolve: true
    // This should attempt to resolve the address but handle the error gracefully
    const exported = await exportCoValue(Person, person.id, {
      resolve: { address: { street: true, city: true } },
      loadAs: alice,
      bestEffortResolution: true,
    });

    assert(exported);

    // Verify the person content is exported
    const personContent = exported.filter((piece) => piece.id === person.id);
    expect(personContent.length).toBeGreaterThan(0);

    const addressContent = exported.filter((piece) => piece.id === address.id);
    expect(addressContent.length).toBeGreaterThan(0);

    const streetContent = exported.filter(
      (piece) => piece.id === address.street.id,
    );
    expect(streetContent).toHaveLength(0);

    const cityContent = exported.filter(
      (piece) => piece.id === address.city.id,
    );
    expect(cityContent).toHaveLength(0);
  });
});

describe("importContentPieces", () => {
  test("imports content pieces successfully", async () => {
    const Person = co.map({
      name: z.string(),
      age: z.number(),
    });

    const group = Group.create();
    const person = Person.create({ name: "John", age: 30 }, group);
    group.addMember("everyone", "reader");

    const alice = await createJazzTestAccount();
    const bob = await createJazzTestAccount();

    bob._raw.core.node.syncManager.getPeers().forEach((peer) => {
      peer.gracefulShutdown();
    });

    // Export from alice's perspective
    const exported = await exportCoValue(Person, person.id, {
      loadAs: alice,
    });

    expect(exported).not.toBeNull();

    // Import to bob's node
    importContentPieces(exported!, bob);

    // Verify bob can now access the person
    const importedPerson = await Person.load(person.id, { loadAs: bob });
    expect(importedPerson).not.toBeNull();
    expect(importedPerson?.name).toBe("John");
    expect(importedPerson?.age).toBe(30);
  });

  test("imports content pieces with nested references", async () => {
    const Address = co.map({
      street: z.string(),
      city: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      address: Address,
    });

    const group = Group.create();
    const address = Address.create(
      { street: "123 Main St", city: "New York" },
      group,
    );
    const person = Person.create({ name: "John", address }, group);
    group.addMember("everyone", "reader");

    const alice = await createJazzTestAccount();
    const bob = await createJazzTestAccount();

    bob._raw.core.node.syncManager.getPeers().forEach((peer) => {
      peer.gracefulShutdown();
    });

    // Export with resolved references
    const exported = await exportCoValue(Person, person.id, {
      resolve: { address: true },
      loadAs: alice,
    });

    expect(exported).not.toBeNull();

    // Import to bob's node
    importContentPieces(exported!, bob);

    // Verify bob can access both person and address
    const importedPerson = await Person.load(person.id, {
      resolve: { address: true },
      loadAs: bob,
    });

    expect(importedPerson).not.toBeNull();
    expect(importedPerson?.name).toBe("John");
    expect(importedPerson?.address).not.toBeNull();
    expect(importedPerson?.address.street).toBe("123 Main St");
    expect(importedPerson?.address.city).toBe("New York");
  });

  test("imports content pieces to anonymous agent", async () => {
    const Person = co.map({
      name: z.string(),
    });

    const group = Group.create();
    const person = Person.create({ name: "John" }, group);
    group.addMember("everyone", "reader");

    const alice = await createJazzTestAccount();
    const { guest } = await createJazzTestGuest();

    guest.node.syncManager.getPeers().forEach((peer) => {
      peer.gracefulShutdown();
    });

    // Export from alice's perspective
    const exported = await exportCoValue(Person, person.id, {
      loadAs: alice,
    });

    expect(exported).not.toBeNull();

    // Import to anonymous agent
    importContentPieces(exported!, guest);

    // Verify anonymous agent can access the person
    const importedPerson = await Person.load(person.id, { loadAs: guest });
    expect(importedPerson).not.toBeNull();
    expect(importedPerson?.name).toBe("John");
  });

  test("imports content pieces without specifying loadAs (uses current account)", async () => {
    const Person = co.map({
      name: z.string(),
    });

    const group = Group.create();
    const person = Person.create({ name: "John" }, group);
    group.addMember("everyone", "reader");

    const alice = await createJazzTestAccount();
    const bob = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    bob._raw.core.node.syncManager.getPeers().forEach((peer) => {
      peer.gracefulShutdown();
    });

    // Export from alice's perspective
    const exported = await exportCoValue(Person, person.id, {
      loadAs: alice,
    });

    expect(exported).not.toBeNull();

    // Import without specifying loadAs (should use current account)
    importContentPieces(exported!);

    // Verify bob can access the person
    const importedPerson = await Person.load(person.id, { loadAs: bob });
    expect(importedPerson).not.toBeNull();
    expect(importedPerson?.name).toBe("John");
  });

  test("handles empty content pieces array", async () => {
    const alice = await createJazzTestAccount();

    // Should not throw when importing empty array
    expect(() => {
      importContentPieces([], alice);
    }).not.toThrow();
  });

  test("handles duplicate content pieces", async () => {
    const Person = co.map({
      name: z.string(),
    });

    const group = Group.create();
    const person = Person.create({ name: "John" }, group);
    group.addMember("everyone", "reader");

    const alice = await createJazzTestAccount();
    const bob = await createJazzTestAccount();

    bob._raw.core.node.syncManager.getPeers().forEach((peer) => {
      peer.gracefulShutdown();
    });

    // Export from alice's perspective
    const exported = await exportCoValue(Person, person.id, {
      loadAs: alice,
    });

    expect(exported).not.toBeNull();

    // Import the same content pieces twice
    importContentPieces(exported!, bob);
    importContentPieces(exported!, bob);

    // Should still work correctly
    const importedPerson = await Person.load(person.id, { loadAs: bob });
    expect(importedPerson).not.toBeNull();
    expect(importedPerson?.name).toBe("John");
  });

  test("imports content pieces with complex nested structure", async () => {
    const Comment = co.map({
      text: z.string(),
      author: z.string(),
    });

    const Post = co.map({
      title: z.string(),
      content: z.string(),
      comments: co.list(Comment),
    });

    const Blog = co.map({
      name: z.string(),
      posts: co.list(Post),
    });

    const group = Group.create();
    const comment1 = Comment.create(
      { text: "Great post!", author: "Alice" },
      group,
    );
    const comment2 = Comment.create({ text: "Thanks!", author: "Bob" }, group);

    const post = Post.create(
      {
        title: "My First Post",
        content: "Hello World",
        comments: Post.def.shape.comments.create([comment1, comment2], group),
      },
      group,
    );

    const blog = Blog.create(
      { name: "My Blog", posts: Blog.def.shape.posts.create([post], group) },
      group,
    );

    group.addMember("everyone", "reader");

    const alice = await createJazzTestAccount();
    const bob = await createJazzTestAccount();

    bob._raw.core.node.syncManager.getPeers().forEach((peer) => {
      peer.gracefulShutdown();
    });

    // Export with all nested references resolved
    const exported = await exportCoValue(Blog, blog.id, {
      resolve: {
        posts: {
          $each: {
            comments: true,
          },
        },
      },
      loadAs: alice,
    });

    expect(exported).not.toBeNull();

    // Import to bob's node
    importContentPieces(exported!, bob);

    // Verify bob can access the entire structure
    const importedBlog = await Blog.load(blog.id, {
      resolve: {
        posts: {
          $each: {
            comments: true,
          },
        },
      },
      loadAs: bob,
    });

    expect(importedBlog).not.toBeNull();
    expect(importedBlog?.name).toBe("My Blog");
    expect(importedBlog?.posts.length).toBe(1);
    expect(importedBlog?.posts[0]?.title).toBe("My First Post");
    expect(importedBlog?.posts[0]?.content).toBe("Hello World");
    expect(importedBlog?.posts[0]?.comments.length).toBe(2);
    expect(importedBlog?.posts[0]?.comments[0]?.text).toBe("Great post!");
    expect(importedBlog?.posts[0]?.comments[0]?.author).toBe("Alice");
    expect(importedBlog?.posts[0]?.comments[1]?.text).toBe("Thanks!");
    expect(importedBlog?.posts[0]?.comments[1]?.author).toBe("Bob");
  });
});
