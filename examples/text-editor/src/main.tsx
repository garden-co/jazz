import { useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { JazzSessionProvider, useDb } from "jazz-tools/react";
import { EditorState, Compartment } from "@codemirror/state";
import { EditorView, keymap, drawSelection } from "@codemirror/view";
import { defaultKeymap } from "@codemirror/commands";
import { yCollab, yUndoManagerKeymap } from "y-codemirror.next";
import * as Y from "yjs";
import { app } from "../schema.js";
import { HEADER } from "./log.js";
import { connect } from "./provider.js";
import "./style.css";

function Editor({ id }: { id: string }) {
  const db = useDb();
  const parent = useRef<HTMLDivElement>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const doc = new Y.Doc();
    const editable = new Compartment();
    const view = new EditorView({
      parent: parent.current!,
      state: EditorState.create({
        extensions: [
          keymap.of([...yUndoManagerKeymap, ...defaultKeymap]),
          drawSelection(),
          EditorView.lineWrapping,
          EditorView.contentAttributes.of({ "aria-label": "Document" }),
          editable.of(EditorView.editable.of(false)),
          yCollab(doc.getText("text"), null),
        ],
      }),
    });
    const disconnect = connect(
      db,
      id,
      doc,
      () => {
        setLoading(false);
        view.dispatch({ effects: editable.reconfigure(EditorView.editable.of(true)) });
      },
      (cause) => {
        setError(String(cause));
        view.dispatch({ effects: editable.reconfigure(EditorView.editable.of(false)) });
      },
    );
    return () => {
      disconnect();
      view.destroy();
      doc.destroy();
    };
  }, [db, id]);

  return (
    <>
      {error ? <p role="alert">{error}</p> : loading && <p>Loading…</p>}
      <div ref={parent} />
    </>
  );
}

function App() {
  const db = useDb();
  const [id, setId] = useState(location.hash.slice(1));
  const [error, setError] = useState("");

  async function create() {
    try {
      const row = await db
        .insert(app.documents, {
          contentLog: HEADER,
        })
        .wait({ tier: "local" });
      history.replaceState(null, "", `#${row.id}`);
      setId(row.id);
    } catch (cause) {
      setError(String(cause));
    }
  }

  return (
    <main>
      <h1>Jazz text editor</h1>
      <p>Open this URL in another browser to share the document. Take turns editing.</p>
      {error && <p role="alert">{error}</p>}
      {id ? <Editor id={id} /> : <button onClick={create}>Create document</button>}
    </main>
  );
}

createRoot(document.getElementById("root")!).render(
  <JazzSessionProvider
    config={{
      appId: import.meta.env.VITE_JAZZ_APP_ID,
      serverUrl: import.meta.env.VITE_JAZZ_SERVER_URL,
      env: "dev",
      initial: "local-first",
    }}
    fallback={<p>Loading…</p>}
  >
    <App />
  </JazzSessionProvider>,
);
