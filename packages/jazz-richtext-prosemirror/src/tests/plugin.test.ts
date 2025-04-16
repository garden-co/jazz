import { Account, CoRichText } from "jazz-tools";
import { createJazzTestAccount, setupJazzTestSync } from "jazz-tools/testing";
import { schema } from "prosemirror-schema-basic";
import { EditorState } from "prosemirror-state";
import { Plugin } from "prosemirror-state";
import { beforeEach, describe, expect, it } from "vitest";
import { createJazzPlugin } from "../lib/plugin";

let account: Account;
let coRichText: CoRichText;
let plugin: Plugin;
let state: EditorState;

beforeEach(async () => {
  await setupJazzTestSync();
  account = await createJazzTestAccount({ isCurrentActiveAccount: true });

  // Create a real CoRichText with the test account as owner
  coRichText = new CoRichText({ text: "<p>Hello</p>", owner: account });

  plugin = createJazzPlugin(coRichText);
  state = EditorState.create({
    schema,
    plugins: [plugin],
  });
});

describe("createJazzPlugin", () => {
  it("initializes editor with CoRichText content", () => {
    expect(state.doc.textContent).toContain("Hello");
  });

  // Additional tests can be adapted to use coRichText
  // For example, you can simulate changes and observe plugin/editor state
});
