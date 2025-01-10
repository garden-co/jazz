import { useAccount } from "jazz-react";
import {
  applyTxToPlainText,
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

  console.log("rerendering");

  useEffect(() => {
    if (!mount) return;

    console.log("Creating EditorView");

    const setupPlugins = exampleSetup({ schema, history: false });
    // console.log("setupPlugins", setupPlugins, schema);

    // Create a new editor view
    const editorView = new EditorView(mount, {
      state: EditorState.create({
        doc: schema.node("doc", undefined, [
          schema.node("paragraph", undefined, undefined),
        ]),
        schema: schema,
        plugins: setupPlugins,
      }),
      dispatchTransaction(tr) {
        const expectedNewState = editorView.state.apply(tr);

        console.log("Applying transaction", lastDoc);
        if (lastDoc) {
          console.log("Applying transaction to plain text");
          applyTxToPlainText(lastDoc, tr);
        }

        console.log("Setting view state to normal new state", expectedNewState);

        editorView.updateState(expectedNewState);
      },
    });

    let lastDoc: Document | undefined;

    console.log("About to subscribe to document:", docID, "with user:", me.id);
    const unsub = Document.subscribe(
      docID,
      me,
      { marks: [{}], text: [] },
      async (doc) => {
        console.log("doc", JSON.parse(JSON.stringify(doc)));

        console.log("doc loaded");

        lastDoc = doc;
        console.log("doc marks", JSON.parse(JSON.stringify(doc.marks)));
        console.log("doc text", JSON.parse(JSON.stringify(doc.text)));

        console.log("Applying doc update");
        console.log(
          "marks",
          doc.toString(),
          doc.resolveAndDiffuseAndFocusMarks(),
        );
        console.log("tree", doc.toTree(["strong", "em"]));

        const focusedBefore = editorView.hasFocus();

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
    console.log("Subscription created successfully");

    return () => {
      console.log("Destroying");
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
