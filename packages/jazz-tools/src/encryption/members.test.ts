import { expect, it, vi } from "vitest";
import { Members } from "./index.js";

it("builds member filters from both parts of the observed space address", async () => {
  const space = { table: "projects", id: "project-1" };
  const address = { scopeId: "projects-table-id", identifier: space.id };
  const resolve = vi.fn(async () => address);
  const members = new Members(resolve);
  expect(await members.where(space)).toEqual(address);
  expect(await members.where(space, "bob")).toEqual({ ...address, accountId: "bob" });
  expect(resolve.mock.calls).toEqual([[space], [space]]);
});
