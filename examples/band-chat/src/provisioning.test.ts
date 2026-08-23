import { expect, it } from "vitest";
import { provisionDemo } from "./provisioning.js";

it("does not write when the demo room already exists", () => {
  const insert = () => {
    throw new Error("read path must not provision");
  };
  expect(provisionDemo({ insert }, "member", { roomId: "existing" })).toBe("existing");
});
