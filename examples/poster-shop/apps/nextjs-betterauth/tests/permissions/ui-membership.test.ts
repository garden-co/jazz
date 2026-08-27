import { expect, it } from "vitest";
import { roleForActiveCanvas } from "../../src/lib/identity.js";

it("selects controls only from the active canvas row owned by the current canonical author", () => {
  const viewer = JSON.stringify(["https://issuer.example", "viewer"]);
  const admin = JSON.stringify(["https://issuer.example", "admin"]);
  const memberships = [
    { canvasId: "active", memberAuthor: admin, role: "admin" as const },
    { canvasId: "active", memberAuthor: viewer, role: "viewer" as const },
    { canvasId: "other", memberAuthor: viewer, role: "admin" as const },
  ];

  expect(roleForActiveCanvas(memberships, "active", viewer)).toBe("viewer");
  expect(roleForActiveCanvas(memberships, "other", admin)).toBeUndefined();
  expect(roleForActiveCanvas(memberships, "active", admin)).toBe("admin");
});
