import { co, CoRichText, z } from "jazz-tools";
import { createJazzPlugin } from "jazz-tools/prosemirror";
import { exampleSetup } from "prosemirror-example-setup";
import { schema } from "prosemirror-schema-basic";
import { EditorState } from "prosemirror-state";
import { EditorView } from "prosemirror-view";

// #region PlainText
const note = co.plainText().create("");

// Create and set up the textarea
const textarea = document.createElement("textarea");
textarea.value = note.toString();

// Add event listener for changes
textarea.addEventListener("input", (e: Event) => {
  const target = e.target as HTMLTextAreaElement;
  // Efficiently update only what the user changed
  note.$jazz.applyDiff(target.value);
});

// Add the textarea to the document
document.body.appendChild(textarea);
// #endregion

// #region RichText
function setupRichTextEditor(
  coRichText: CoRichText,
  container: HTMLDivElement,
) {
  // Create the Jazz plugin for ProseMirror
  // Providing a co.richText() instance to the plugin to automatically sync changes
  const jazzPlugin = createJazzPlugin(coRichText); // [!code ++]

  // Set up ProseMirror with Jazz plugin
  const view = new EditorView(container, {
    state: EditorState.create({
      schema,
      plugins: [
        ...exampleSetup({ schema }),
        jazzPlugin, // [!code ++]
      ],
    }),
  });

  // Return cleanup function
  return () => {
    view.destroy();
  };
}

// Usage
const doc = co.richText().create("<p>Initial content</p>");
const editorContainer = document.getElementById("editor") as HTMLDivElement;
const cleanup = setupRichTextEditor(doc, editorContainer);

// Later when done with the editor
cleanup();
// #endregion
