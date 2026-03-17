import { useEffect, useRef, useState, useCallback, forwardRef, useImperativeHandle } from "react";
import { EditorState, Plugin } from "prosemirror-state";
import { EditorView, Decoration, DecorationSet } from "prosemirror-view";
import { DOMSerializer } from "prosemirror-model";
import { keymap } from "prosemirror-keymap";
import { baseKeymap } from "prosemirror-commands";
import { history } from "prosemirror-history";
import { schema, editorKeymap } from "./utils";
import type { FloatingToolbarState } from "./floatingToolbarPlugin";
import { FloatingToolbar } from "./FloatingToolbar";

export interface EditorHandle {
  send: () => void;
  insertText: (text: string) => void;
}

interface EditorProps {
  onSend: (html: string) => void;
  placeholder?: string;
  disabled?: boolean;
}

function serialiseToHtml(state: EditorState): string {
  const fragment = DOMSerializer.fromSchema(schema).serializeFragment(state.doc.content);
  const div = document.createElement("div");
  div.appendChild(fragment);
  return div.innerHTML;
}

function isDocEmpty(state: EditorState): boolean {
  const { doc } = state;
  return doc.childCount === 1 && doc.firstChild!.isTextblock && doc.firstChild!.content.size === 0;
}

function placeholderPlugin(text: string) {
  return new Plugin({
    props: {
      decorations(state) {
        if (!isDocEmpty(state)) return DecorationSet.empty;
        const placeholder = document.createElement("span");
        placeholder.className = "text-muted-foreground pointer-events-none";
        placeholder.textContent = text;
        return DecorationSet.create(state.doc, [Decoration.widget(1, placeholder, { side: 0 })]);
      },
    },
  });
}

export const Editor = forwardRef<EditorHandle, EditorProps>(function Editor(
  { onSend, placeholder = "Type a message...", disabled },
  ref,
) {
  const editorRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const [toolbarState, setToolbarState] = useState<FloatingToolbarState>({
    visible: false,
    top: 0,
    left: 0,
  });

  const handleSend = useCallback(() => {
    const view = viewRef.current;
    if (!view) return;
    if (isDocEmpty(view.state)) return;

    const html = serialiseToHtml(view.state);
    onSend(html);

    const newState = EditorState.create({ schema, plugins: view.state.plugins });
    view.updateState(newState);
  }, [onSend]);

  const handleInsertText = useCallback((text: string) => {
    const view = viewRef.current;
    if (!view) return;
    const tr = view.state.tr.insertText(text);
    view.dispatch(tr);
  }, []);

  const handle: EditorHandle = { send: handleSend, insertText: handleInsertText };

  useImperativeHandle(ref, () => handle, [handleSend, handleInsertText]);

  // Expose the handle on the DOM element for browser tests.
  useEffect(() => {
    const el = containerRef.current;
    if (el) (el as unknown as Record<string, unknown>).__editorHandle = handle;
  });

  useEffect(() => {
    if (!editorRef.current) return;

    const state = EditorState.create({
      schema,
      plugins: [
        editorKeymap(handleSend),
        keymap(baseKeymap),
        history(),
        placeholderPlugin(placeholder),
      ],
    });

    const view = new EditorView(editorRef.current, {
      state,
      editable: () => !disabled,
      dispatchTransaction(tr) {
        const newState = view.state.apply(tr);
        view.updateState(newState);

        const { from, to, empty } = newState.selection;
        if (empty) {
          setToolbarState({ visible: false, top: 0, left: 0 });
        } else {
          const container = containerRef.current;
          if (container) {
            const start = view.coordsAtPos(from);
            const end = view.coordsAtPos(to);
            const containerRect = container.getBoundingClientRect();
            setToolbarState({
              visible: true,
              top: start.top - containerRect.top - 40,
              left: (start.left + end.left) / 2 - containerRect.left,
            });
          }
        }
      },
      attributes: {
        class:
          "flex-1 rounded-md border bg-background px-3 py-2 text-sm min-h-[40px] max-h-[120px] overflow-y-auto focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring prose prose-sm max-w-none",
      },
    });

    viewRef.current = view;

    return () => {
      view.destroy();
      viewRef.current = null;
    };
  }, [disabled, handleSend]);

  return (
    <div ref={containerRef} id="messageEditor" className="relative flex-1">
      <div ref={editorRef} />
      {viewRef.current && <FloatingToolbar state={toolbarState} view={viewRef.current} />}
    </div>
  );
});
