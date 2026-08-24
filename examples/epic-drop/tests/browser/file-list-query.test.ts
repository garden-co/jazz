import { describe, expect, it } from "vitest";
import { fileListQuery } from "../../src/file-list-query.js";

describe("EpicDrop file-list query", () => {
  it("does not create a folder filter before the initial folder selection", () => {
    expect(fileListQuery(undefined)).toBeUndefined();
    expect(fileListQuery("")).toBeUndefined();
  });
});
