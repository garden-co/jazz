import { describe, expect, it } from "vitest";
import { asLines, rebaseText, textToSave } from "../../src/lib/text-rebase.js";

describe("saving a stop's text after a bandmate edited it", () => {
  const opened = "Load-in at 2pm. Rider: extra monitors stage left.";

  it("keeps the bandmate's edit and the form's edit", () => {
    const form = "Load-in at 2pm. Rider: extra monitors stage left. **Bring the spare snare.**";
    const bandmate = "Load-in at 3pm. Rider: extra monitors stage left.";
    expect(rebaseText(opened, form, bandmate)).toBe(
      "Load-in at 3pm. Rider: extra monitors stage left. **Bring the spare snare.**",
    );
  });

  it("saves the bandmate's text unchanged when the form changed nothing", () => {
    expect(rebaseText(opened, opened, "Load-in moved to 4pm.")).toBe("Load-in moved to 4pm.");
  });

  it("lets the form win where both changed the same words", () => {
    expect(rebaseText(opened, opened.replace("2pm", "5pm"), opened.replace("2pm", "3pm"))).toBe(
      opened.replace("2pm", "5pm"),
    );
  });

  it("puts the bandmate's insertion first when both added at the same spot", () => {
    expect(rebaseText("ab", "aFb", "aBb")).toBe("aBFb");
  });
});

describe("what the stop form saves", () => {
  it("ends a text with one line break, and saves an empty one as empty", () => {
    expect(asLines("Doors at 8.  \n\n")).toBe("Doors at 8.\n");
    expect(asLines(" \n ")).toBe("");
    expect(textToSave("", "", "")).toBe("");
  });

  it("keeps two bandmates' first notes as separate lines", () => {
    // The bandmate's note arrived while the form was open on an empty note.
    const bandmate = textToSave("", "Bring the spare snare.", "");
    expect(textToSave("", "Merch table by the door.", bandmate)).toBe(
      "Bring the spare snare.\nMerch table by the door.\n",
    );
  });

  it("carries the form's edit over to a bandmate's newer text", () => {
    const opened = "Load-in at 2pm.\n";
    const bandmate = "Load-in at 3pm.\n";
    expect(textToSave(opened, "Load-in at 2pm.\nSoundcheck at 5pm.", bandmate)).toBe(
      "Load-in at 3pm.\nSoundcheck at 5pm.\n",
    );
  });

  it("saves the form's text when nobody else edited", () => {
    expect(textToSave("Doors at 8.", "Doors at 9.", "Doors at 8.")).toBe("Doors at 9.\n");
  });
});
