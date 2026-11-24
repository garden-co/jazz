import { expect, it, vi } from "vitest";
import * as producer from "./unreadable.js";

it("shares unreadable rows across independently loaded SDK copies", async () => {
  const omission = { table: "projects", id: "missing", column: "title", code: "key-pending" };
  const first = producer.withUnreadable([], [omission]);
  vi.resetModules();
  const consumer = await import("./unreadable.js");
  expect(consumer.unreadableRows).not.toBe(producer.unreadableRows);

  const second = consumer.withUnreadable([], []);
  expect(consumer.unreadableRows(first)).toEqual([omission]);
  expect(producer.unreadableRows(second)).toEqual([]);
  expect(consumer.unreadableRows(first)).toEqual([omission]);
});
