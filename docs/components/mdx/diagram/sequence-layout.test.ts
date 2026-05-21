import { describe, expect, it } from "vitest";

import { layoutSequence, wrapText } from "./sequence-layout";

describe("wrapText", () => {
  it("keeps short text on one line", () => {
    expect(wrapText("Sign in", 30)).toEqual(["Sign in"]);
  });

  it("wraps at word boundaries when the line is too long", () => {
    expect(wrapText("Verify proof, create user with same Jazz ID", 22)).toEqual([
      "Verify proof, create",
      "user with same Jazz ID",
    ]);
  });

  it("never splits a single unbreakable token", () => {
    expect(wrapText("db.getLocalFirstIdentityProof()", 10)).toEqual([
      "db.getLocalFirstIdentityProof()",
    ]);
  });

  it("returns a single empty line for empty text", () => {
    expect(wrapText("", 10)).toEqual([""]);
  });
});

describe("layoutSequence — long self-message", () => {
  const narrow = layoutSequence(
    [
      { id: "client", label: "Client" },
      { id: "provider", label: "Provider server" },
    ],
    [
      {
        kind: "message",
        from: "provider",
        to: "provider",
        text: "Verify proof, create user with same Jazz ID",
      },
    ],
  );
  const baseline = layoutSequence(
    [
      { id: "client", label: "Client" },
      { id: "provider", label: "Provider server" },
    ],
    [{ kind: "message", from: "provider", to: "provider", text: "Verify" }],
  );

  it("wraps the label instead of inflating canvas width", () => {
    // A 43-char label must not widen the canvas much beyond the short one.
    expect(narrow.width - baseline.width).toBeLessThan(140);
  });

  it("grows that row's height to fit the wrapped lines", () => {
    expect(narrow.height).toBeGreaterThan(baseline.height);
  });

  it("exposes the wrapped lines per step", () => {
    expect(narrow.stepLines[0].length).toBeGreaterThan(1);
  });
});

describe("layoutSequence", () => {
  const { actors, stepY, width, height } = layoutSequence(
    [
      { id: "A", label: "Browser" },
      { id: "B", label: "Auth" },
      { id: "C", label: "Jazz", createAtStep: 2 },
    ],
    [
      { kind: "message", from: "A", to: "B", text: "Sign in" },
      { kind: "message", from: "B", to: "A", text: "JWT", line: "dashed" },
      { kind: "note", over: "A", text: "starts here" },
      { kind: "message", from: "A", to: "C", text: "Connect" },
    ],
  );

  it("places actor lifelines left-to-right by declaration order", () => {
    expect(actors.A.cx).toBeLessThan(actors.B.cx);
    expect(actors.B.cx).toBeLessThan(actors.C.cx);
  });

  it("sizes actor boxes from the label with a minimum", () => {
    expect(actors.A.boxW).toBe(70); // "Browser" 7*8 + 14
    expect(actors.B.boxW).toBe(60); // "Auth" → clamped to min 60
  });

  it("rows one step per index below the header", () => {
    expect(stepY).toEqual([90, 138, 186, 234]);
  });

  it("drops a created participant in at its step row, lifeline from there", () => {
    expect(actors.A.boxY).toBe(8); // header
    expect(actors.A.lifeTop).toBe(42);
    expect(actors.C.boxY).toBe(169); // centred on stepY[2]=186
    expect(actors.C.lifeTop).toBe(203);
  });

  it("exposes createAtStep so the renderer can land messages on the box edge", () => {
    // Without this the renderer can't tell that a message at stepY[2] is the
    // creation of C, so its arrowhead ends up centred on C's lifeline — hidden
    // behind the C box (drawn last). The fix lives in the renderer, but it
    // needs the layout to surface the step index.
    expect(actors.A.createAtStep).toBeUndefined();
    expect(actors.B.createAtStep).toBeUndefined();
    expect(actors.C.createAtStep).toBe(2);
  });

  it("computes a canvas sized to bound notes/labels, not just columns", () => {
    expect(width).toBe(363); // participant span widened + offset so the note can't clip
    expect(height).toBe(282);
  });
});
