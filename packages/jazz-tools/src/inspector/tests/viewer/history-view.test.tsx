// @vitest-environment happy-dom
import { afterEach, assert, beforeAll, describe, expect, it } from "vitest";
import {
  assertLoaded,
  createJazzTestAccount,
  setupJazzTestSync,
} from "jazz-tools/testing";
import { co, z } from "jazz-tools";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { HistoryView } from "../../viewer/history-view";
import { setup } from "goober";
import React from "react";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function extractAction(row: HTMLElement | null | undefined) {
  if (!row) return "";
  // index 0: author, index 1: action, index 2: timestamp
  return row.querySelectorAll("td")?.[1]?.textContent ?? "";
}

function extractActions(): string[] {
  // slice 2 to skip header and filters
  return screen.getAllByRole("row").slice(2).map(extractAction);
}

describe("HistoryView", async () => {
  const account = await setupJazzTestSync();

  beforeAll(() => {
    // setup goober
    setup(React.createElement);
  });

  afterEach(() => {
    cleanup();
  });

  it("should render a history card", async () => {
    const value = co
      .map({
        foo: z.string(),
      })
      .create({ foo: "bar" }, account);

    render(
      <HistoryView coValue={value.$jazz.raw} node={account.$jazz.localNode} />,
    );

    expect(
      screen.getAllByText('Property "foo" has been set to "bar"'),
    ).toHaveLength(1);
  });

  describe("co.map", () => {
    it("should render co.map changes", async () => {
      const value = co
        .map({
          pet: z.string(),
          age: z.number(),
          certified: z.boolean().optional(),
        })
        .create({ pet: "dog", age: 10, certified: false }, account);

      value.$jazz.set("pet", "cat");
      value.$jazz.set("age", 20);
      value.$jazz.set("certified", true);
      value.$jazz.delete("certified");

      render(
        <HistoryView
          coValue={value.$jazz.raw}
          node={account.$jazz.localNode}
        />,
      );

      const history = [
        'Property "pet" has been set to "dog"',
        'Property "age" has been set to 10',
        'Property "certified" has been set to false',
        'Property "pet" has been set to "cat"',
        'Property "age" has been set to 20',
        'Property "certified" has been set to true',
        'Property "certified" has been deleted',
      ].toReversed(); // Default sort is descending

      await waitFor(() => {
        expect(screen.getAllByRole("row")[2]?.textContent).toContain(
          account.$jazz.id,
        );
      });

      expect(extractActions()).toEqual(history);
    });

    it("should render invalid changes", async () => {
      const account2 = await createJazzTestAccount();
      const group = co.group().create(account);
      group.addMember(account2, "reader");

      const Schema = co.map({
        pet: z.string(),
        age: z.number(),
        certified: z.boolean().optional(),
      });

      const value = Schema.create(
        { pet: "dog", age: 10, certified: false },
        group,
      );

      const valueOnAccount2 = await Schema.load(value.$jazz.id, {
        loadAs: account2,
      });
      assertLoaded(valueOnAccount2);

      // This is invalid, since account2 is a reader
      valueOnAccount2.$jazz.set("pet", "cat");

      render(
        <HistoryView
          coValue={valueOnAccount2.$jazz.raw}
          node={account2.$jazz.localNode}
        />,
      );

      const history = [
        'Property "pet" has been set to "dog"',
        'Property "age" has been set to 10',
        'Property "certified" has been set to false',

        // Account2 can't write to the value
        'Property "pet" has been set to "cat"Invalid transaction: Transactor has no write permissions',
      ].toReversed(); // Default sort is descending

      await waitFor(() => {
        expect(screen.getAllByRole("row")[2]?.textContent).toContain(
          account2.$jazz.id,
        );
      });

      expect(extractActions()).toEqual(history);
    });

    it("should render co.map changes with json", async () => {
      const d = new Date();
      const value = co
        .map({
          pet: z.object({
            name: z.string(),
            age: z.number(),
          }),
          d: z.date(),
          n: z.number().optional(),
          s: z.string().nullable(),
        })
        .create(
          { pet: { name: "dog", age: 10 }, d, n: 10, s: "hello" },
          account,
        );

      value.$jazz.set("pet", { name: "cat", age: 20 });
      value.$jazz.set("n", undefined);
      value.$jazz.set("s", null);
      render(
        <HistoryView coValue={value.$jazz.raw} node={value.$jazz.localNode} />,
      );

      const history = [
        'Property "pet" has been set to {"name":"dog","age":10}',
        `Property "d" has been set to "${d.toISOString()}"`,
        'Property "n" has been set to 10',
        'Property "s" has been set to "hello"',
        'Property "pet" has been set to {"name":"cat","age":20}',
        'Property "n" has been set to undefined',
        'Property "s" has been set to null',
      ].toReversed(); // Default sort is descending

      await waitFor(() => {
        expect(screen.getAllByRole("row")[2]?.textContent).toContain(
          account.$jazz.id,
        );
      });

      expect(extractActions()).toEqual(history);
    });
  });

  describe("co.list", () => {
    it("should render simple co.list changes", async () => {
      const value = co.list(z.string()).create(["dog", "cat"], account);

      value.$jazz.push("bird");

      value.$jazz.splice(1, 0, "fish");

      value.$jazz.shift();

      render(
        <HistoryView
          coValue={value.$jazz.raw}
          node={account.$jazz.localNode}
        />,
      );

      const history = [
        '"dog" has been appended',
        '"cat" has been appended',
        '"bird" has been inserted after "cat"',
        '"fish" has been inserted after "dog"',
        '"dog" has been deleted',
      ].toReversed(); // Default sort is descending

      expect(extractActions()).toEqual(history);
    });

    it("should render changes of a co.list of co.maps", async () => {
      const Animal = co.map({
        pet: z.string(),
        age: z.number(),
        certified: z.boolean(),
      });

      const dog = Animal.create(
        { pet: "dog", age: 10, certified: false },
        account,
      );
      const cat = Animal.create(
        { pet: "cat", age: 20, certified: true },
        account,
      );
      const fish = Animal.create(
        { pet: "fish", age: 30, certified: false },
        account,
      );
      const bird = Animal.create(
        { pet: "bird", age: 40, certified: true },
        account,
      );

      const value = co.list(Animal).create([dog, cat], account);

      value.$jazz.push(bird);

      value.$jazz.splice(1, 0, fish);

      value.$jazz.shift();

      render(
        <HistoryView
          coValue={value.$jazz.raw}
          node={account.$jazz.localNode}
        />,
      );

      const history = [
        `"${dog.$jazz.id}" has been appended`,
        `"${cat.$jazz.id}" has been appended`,
        `"${bird.$jazz.id}" has been inserted after "${cat.$jazz.id}"`,
        `"${fish.$jazz.id}" has been inserted after "${dog.$jazz.id}"`,
        `"${dog.$jazz.id}" has been deleted`,
      ].toReversed(); // Default sort is descending

      expect(extractActions()).toEqual(history);
    });
  });

  describe("co.plaintext", () => {
    it("should render co.plaintext initial append in a single row", async () => {
      const value = co.plainText().create("hello", account);
      render(
        <HistoryView coValue={value.$jazz.raw} node={value.$jazz.localNode} />,
      );

      expect(extractActions()).toEqual(['"hello" has been appended']);
    });

    it("should render co.plaintext appends in a single row", async () => {
      const value = co.plainText().create("hello", account);
      value.$jazz.applyDiff("hello world");
      value.$jazz.applyDiff("hello world!");

      expect(value.$jazz.raw.toString()).toEqual("hello world!");

      render(
        <HistoryView coValue={value.$jazz.raw} node={value.$jazz.localNode} />,
      );

      const history = [
        '"hello" has been appended',
        '" world" has been inserted after "o"',
        '"!" has been inserted after " "', // it is after " " because previous action is reversed
      ].toReversed(); // Default sort is descending

      expect(extractActions()).toEqual(history);
    });

    it("should render co.plaintext delete in tail", async () => {
      const value = co.plainText().create("hello", account);
      value.$jazz.applyDiff("hell");

      expect(value.$jazz.raw.toString()).toEqual("hell");

      render(
        <HistoryView coValue={value.$jazz.raw} node={value.$jazz.localNode} />,
      );

      const history = [
        '"hello" has been appended',
        '"o" has been deleted',
      ].toReversed(); // Default sort is descending

      expect(extractActions()).toEqual(history);
    });

    it("should render co.plaintext delete in head", async () => {
      const value = co.plainText().create("hello", account);
      value.$jazz.applyDiff("ello");

      expect(value.$jazz.raw.toString()).toEqual("ello");

      render(
        <HistoryView coValue={value.$jazz.raw} node={value.$jazz.localNode} />,
      );

      const history = [
        '"hello" has been appended',
        '"h" has been deleted',
      ].toReversed(); // Default sort is descending

      expect(extractActions()).toEqual(history);
    });

    it("should render co.plaintext delete history of multiple old insertions in a single row", async () => {
      const value = co.plainText().create("hello", account);
      await sleep(2);
      value.$jazz.applyDiff("hello world");
      await sleep(2);
      value.$jazz.applyDiff("hed");

      expect(value.$jazz.raw.toString()).toEqual("hed");

      render(
        <HistoryView coValue={value.$jazz.raw} node={value.$jazz.localNode} />,
      );

      const history = [
        '"hello" has been appended',
        '" world" has been inserted after "o"',
        '"lod" has been deleted',
        '" worl" has been deleted',
      ].toReversed(); // Default sort is descending

      expect(extractActions()).toEqual(history);
    });

    it("should render co.plaintext insertBefore in history", async () => {
      const value = co.plainText().create("world", account);
      await sleep(2);
      value.insertBefore(0, "Hello, ");

      expect(value.$jazz.raw.toString()).toEqual("Hello, world");

      render(
        <HistoryView coValue={value.$jazz.raw} node={value.$jazz.localNode} />,
      );

      const history = [
        '"world" has been appended',
        '"H" has been inserted before "w"',
        '"ello, " has been inserted after "H"',
      ].toReversed(); // Default sort is descending

      expect(extractActions()).toEqual(history);
    });

    it("should render co.plaintext insertAfter in history", async () => {
      const value = co.plainText().create("world", account);
      await sleep(2);
      value.insertAfter(0, "Hello, ");

      expect(value.$jazz.raw.toString()).toEqual("wHello, orld");

      render(
        <HistoryView coValue={value.$jazz.raw} node={value.$jazz.localNode} />,
      );

      const history = [
        '"world" has been appended',
        '"Hello, " has been inserted after "w"',
      ].toReversed(); // Default sort is descending

      expect(extractActions()).toEqual(history);
    });
  });

  describe("co.group", async () => {
    const account2 = await createJazzTestAccount();

    it("should render co.group changes", async () => {
      const group = co.group().create(account);

      const group2 = co.group().create(account);

      group.addMember(group2, "writer");

      group.addMember(account2, "reader");
      group.removeMember(account2);

      const group3 = co.group().create(account);
      group3.addMember(group, "inherit");

      render(
        <HistoryView
          coValue={group.$jazz.raw}
          node={account.$jazz.localNode}
        />,
      );

      const history = [
        `${account.$jazz.id} has been promoted to admin`,
        expect.stringContaining(` has been revealed to `), // key revelation
        expect.stringContaining('Property "readKey" has been set to'),
        expect.stringContaining('Property "groupSealer" has been set to'),
        `Group ${group2.$jazz.id} has been promoted to writer`,
        expect.stringContaining(" has been revealed to"),
        `${account2.$jazz.id} has been promoted to reader`,
        expect.stringContaining(" has been revealed to"),
        // Member revocation: key rotation
        expect.stringContaining(" has been revealed to"),
        expect.stringContaining(" has been revealed to"),
        expect.stringContaining('Property "readKey" has been set to'),
        expect.stringContaining('Property "groupSealer" has been set to'),
        expect.stringContaining(" has been revealed to"),
        `${account2.$jazz.id} has been revoked`,

        // Group extension
        // `Group become a member of ${group3.$jazz.id}`,
      ].toReversed(); // Default sort is descending

      const historyPage1 = history.slice(0, 10);
      const historyPage2 = history.slice(10, 20);

      // Page 1: 10 rows
      expect(extractActions()).toEqual(historyPage1);

      // Go to page 2
      fireEvent.click(screen.getByText("»"));

      // Page 2: 3 rows
      expect(extractActions()).toEqual(historyPage2);
    });
  });

  describe("co.account", () => {
    it("should render co.account changes", async () => {
      const account = await createJazzTestAccount({
        creationProps: {
          name: "John Doe",
        },
      });

      const history = [
        expect.stringContaining(' has been set to "admin"'),
        expect.stringContaining(" has been revealed to "),
        expect.stringContaining('Property "readKey" has been set to '),
        `Property "profile" has been set to "${account.profile!.$jazz.id}"`,
      ].toReversed(); // Default sort is descending

      render(
        <HistoryView
          coValue={account.$jazz.raw}
          node={account.$jazz.localNode}
        />,
      );

      expect(extractActions()).toEqual(history);
    });
  });
});
