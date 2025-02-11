import { createJazzPlugin } from "jazz-browser-prosemirror";
import { useAccount } from "jazz-react";
import { CoRichText, ID } from "jazz-tools";
import { exampleSetup } from "prosemirror-example-setup";
import { schema } from "prosemirror-schema-basic";
import { EditorState } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import { useEffect, useState } from "react";
import "prosemirror-example-setup/style/style.css";
import "prosemirror-menu/style/menu.css";
import "prosemirror-view/style/prosemirror.css";

interface DocumentProps {
  docId?: ID<CoRichText>;
}

export function Document({ docId }: DocumentProps) {
  const { me } = useAccount();
  const [mount, setMount] = useState<HTMLElement | null>(null);
  const [_doc, setDoc] = useState<CoRichText | null>(null);

  useEffect(() => {
    if (!mount) return;

    const setupPlugins = exampleSetup({ schema, history: false });

    async function initializeDocument() {
      try {
        const loadedDoc = docId
          ? await CoRichText.load(docId, me, { text: [], marks: [{}] })
          : CoRichText.createFromPlainText("", { owner: me });

        if (!loadedDoc) return;
        setDoc(loadedDoc);

        const jazzPlugin = createJazzPlugin(loadedDoc);
        const editorView = new EditorView(mount, {
          state: EditorState.create({
            doc: schema.node("doc", undefined, [
              schema.node("paragraph", undefined, undefined),
            ]),
            schema,
            plugins: [...setupPlugins, jazzPlugin],
          }),
        });

        return () => editorView.destroy();
      } catch (error) {
        console.error("Failed to initialize document:", error);
      }
    }

    initializeDocument();
  }, [mount, docId, me]);

  return <div ref={setMount} className="min-w-96" />;
}
