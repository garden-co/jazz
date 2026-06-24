import { useEffect } from "react";
import type * as monaco from "monaco-editor";
import * as Y from "yjs";
// y-monaco's MonacoBinding keeps a Monaco text model in sync with a Y.Text.
// We pass no awareness argument — this example has no live cursors.
import { MonacoBinding } from "y-monaco";

type UseMonacoBindingArgs = {
  editor: monaco.editor.IStandaloneCodeEditor | null;
  monaco: typeof import("monaco-editor") | null;
  ydoc: Y.Doc;
};

export function useMonacoBinding({ editor, monaco, ydoc }: UseMonacoBindingArgs) {
  useEffect(() => {
    if (!editor || !monaco) return;

    const model = editor.getModel();
    if (!model) return;

    const binding = new MonacoBinding(ydoc.getText("monaco"), model, new Set([editor]));
    return () => binding.destroy();
  }, [editor, monaco, ydoc]);
}
