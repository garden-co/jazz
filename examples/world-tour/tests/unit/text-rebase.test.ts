import { describe, expect, it } from "vitest";
import { rebaseText } from "../../src/lib/text-rebase.js";

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
