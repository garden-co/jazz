import { useAccount } from "jazz-react";
import {
  applyTrToRichText,
  richTextToProsemirrorDoc,
} from "jazz-richtext-prosemirror";
import { ID } from "jazz-tools";
import { exampleSetup } from "prosemirror-example-setup";
import { schema } from "prosemirror-schema-basic";
import { EditorState } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import { useEffect, useState } from "react";
import { Document } from "./schema";

import "prosemirror-example-setup/style/style.css";
import "prosemirror-menu/style/menu.css";
import "prosemirror-view/style/prosemirror.css";

/**
 * Component that integrates CoRichText with ProseMirror editor.
 * Handles bidirectional synchronization between Jazz and ProseMirror states.
 *
 * @param docID - The ID of the document to edit
 */
export function DocumentComponent({ docID }: { docID: ID<Document> }) {
  const { me } = useAccount();
  const [mount, setMount] = useState<HTMLElement | null>(null);

  useEffect(() => {
    if (!mount) return;

    const setupPlugins = exampleSetup({ schema, history: false });

    // Create a new editor view
    const editorView = new EditorView(mount, {
      state: EditorState.create({
        doc: schema.node("doc", undefined, [
          schema.node("paragraph", undefined, undefined),
        ]),
        schema: schema,
        plugins: setupPlugins,
      }),
      dispatchTransaction(transaction) {
        const expectedNewState = editorView.state.apply(transaction);

        if (lastDoc) {
          applyTrToRichText(lastDoc, transaction);
        }

        editorView.updateState(expectedNewState);
      },
    });

    let lastDoc: Document | undefined;

    // Subscribe to document updates
    const unsub = Document.subscribe(
      docID,
      me,
      { marks: [{}], text: [] },
      async (doc) => {
        lastDoc = doc;

        // Check if the editor is currently focused
        const focusedBefore = editorView.hasFocus();

        // Update the editor state
        editorView.updateState(
          EditorState.create({
            doc: richTextToProsemirrorDoc(doc),
            plugins: editorView.state.plugins,
            selection: editorView.state.selection,
            schema: editorView.state.schema,
            storedMarks: editorView.state.storedMarks,
          }),
        );

        // Focus the editor after the state has been updated
        if (focusedBefore) {
          setTimeout(() => {
            editorView.focus();
          }, 0);
        }
      },
    );

    // Clean up on unmount
    return () => {
      editorView.destroy();
      unsub();
    };
  }, [mount, docID, !!me]);

  return (
    <div>
      <h1>Document</h1>
      <div ref={setMount} className="border" />
    </div>
  );
}
