// Vercel Web Analytics custom event for copies: the agent quickstart (button
// or keyboard) and any other text or code people copy from the site.
//
// Vercel Pro allows two properties per custom event, so the event carries what
// was copied and how. Vercel records the page with every event.

export const COPY_EVENT = "Copy";

export type CopyWhat = "quickstart" | "quickstart-partial" | "code" | "text";
export type CopyMethod = "button" | "keyboard" | "menu";

/** How long after Cmd/Ctrl+C a copy event still counts as a keyboard copy. */
export const KEYBOARD_COPY_WINDOW_MS = 1000;

/** Selecting this much of the quickstart counts as copying all of it. */
const FULL_QUICKSTART_RATIO = 0.95;

/** Marks the element that shows the agent quickstart: its `<pre>` in AgentPrompt. */
export const QUICKSTART_ATTRIBUTE = "data-agent-quickstart";

export function copyWhat(
  context: { inQuickstart: boolean; inCode: boolean },
  selectedChars: number,
  quickstartChars: number,
): CopyWhat {
  if (context.inQuickstart) {
    return selectedChars >= quickstartChars * FULL_QUICKSTART_RATIO
      ? "quickstart"
      : "quickstart-partial";
  }
  return context.inCode ? "code" : "text";
}

export function copyEvent(what: CopyWhat, method: CopyMethod) {
  return { name: COPY_EVENT, properties: { what, method } };
}
