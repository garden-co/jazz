import type { co } from "jazz-tools";
import type { Message } from "@/schema";
import { type RefObject, useEffect, useRef, useState } from "react";
import { createJazzPlugin } from "jazz-tools/prosemirror";
import { baseKeymap } from "prosemirror-commands";
import { history } from "prosemirror-history";
import { keymap } from "prosemirror-keymap";
import { EditorState } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import { FloatingToolbar } from "./FloatingToolbar";
import { createFloatingToolbarPlugin } from "./floatingToolbarPlugin";
import {
  createEditorKeymap,
  createEditorSchema,
  INITIAL_MENU_STATE,
  type MenuState,
} from "./utils";

interface EditorProps {
  message: RefObject<co.loaded<typeof Message>>;
  onEnter: () => void;
}

export function Editor({ message, onEnter }: EditorProps) {
  const editorRef = useRef<HTMLDivElement>(null);
  const [view, setView] = useState<EditorView | null>(null);
  const [menuState, setMenuState] = useState<MenuState>(INITIAL_MENU_STATE);

  useEffect(() => {
    if (!editorRef.current || !message.current) return;

    const schema = createEditorSchema();
    const jazzPlugin = createJazzPlugin(message.current.text);
    const customKeymap = createEditorKeymap(schema, onEnter);
    const toolbarPlugin = createFloatingToolbarPlugin(setMenuState);

    const state = EditorState.create({
      schema,
      plugins: [
        history(),
        customKeymap,
        keymap(baseKeymap),
        jazzPlugin,
        toolbarPlugin,
      ],
    });

    const editorView = new EditorView(editorRef.current, { state });
    setView(editorView);
    editorView.focus();

    return () => editorView.destroy();
  }, [message.current, onEnter]);

  return (
    <div className="relative flex-1 min-w-0">
      <div
        ref={editorRef}
        className="w-full max-w-full rounded-md border bg-background p-2 [&_.ProseMirror]:outline-none [&_.ProseMirror]:prose-sm [&_.ProseMirror]:whitespace-pre-wrap [&_.ProseMirror]:wrap-break-word [&_.ProseMirror]:max-w-full"
        id="messageEditor"
      />
      <br />
      {message.current.text}
      <FloatingToolbar view={view} menuState={menuState} />
    </div>
  );
}
