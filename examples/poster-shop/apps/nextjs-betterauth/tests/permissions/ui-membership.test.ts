import { expect, it } from "vitest";
import { roleForActiveCanvas } from "../../src/lib/identity.js";

it("selects controls only from the active canvas row owned by the current canonical author", () => {
  const viewer = "00000000-0000-4000-8000-000000000001";
  const admin = "00000000-0000-4000-8000-000000000002";
  const memberships = [
    { canvasId: "active", memberAuthor: admin, role: "admin" as const },
    { canvasId: "active", memberAuthor: viewer, role: "viewer" as const },
    { canvasId: "other", memberAuthor: viewer, role: "admin" as const },
  ];

  expect(roleForActiveCanvas(memberships, "active", viewer)).toBe("viewer");
  expect(roleForActiveCanvas(memberships, "other", admin)).toBeUndefined();
  expect(roleForActiveCanvas(memberships, "active", admin)).toBe("admin");
});
