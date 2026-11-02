"use client";

import { useEffect, useImperativeHandle, useRef, type Ref } from "react";
import { Selection } from "@tiptap/pm/state";
import { Placeholder } from "@tiptap/extensions";
import { EditorContent, useEditor, type Editor } from "@tiptap/react";
import { docToMarkdown, markdownToDoc, rebaseText, richTextExtensions } from "@/src/lib/rich-text";
import type { InlineTextVariant } from "./InlineText";

/** While typing, the block's text is written to Jazz at most this often. */
const WRITE_INTERVAL_MS = 300;
const FROM_JAZZ = "bandbook/from-jazz";

export type RichTextHandle = {
  focus: (at: "start" | "end") => void;
  /** Write any edits still waiting for the next write. */
  flush: () => void;
  plainText: () => string;
  /**
   * Remove the first `count` characters (a typed shortcut such as `# `) and
   * return the Markdown left. The caller writes it, so it is not written here.
   */
  takeLeading: (count: number) => string;
};

export type RichTextKeyInfo = { isEmpty: boolean; caretAtStart: boolean; caretAtEnd: boolean };

/**
 * One block's text as an inline rich text editor, stored as Markdown in a
 * text merge column.
 *
 * The first edit is written at once and later ones at most every 300 ms while
 * typing continues. Each write sends the whole Markdown value, and the server
 * merges it with edits other people made meanwhile. A value that
 * arrives from Jazz is applied as the smallest change to the document, so the
 * cursor stays where it was. If it arrives while edits are still waiting, they
 * are carried over onto it first and written straight away.
 */
export function RichText({
  value,
  label,
  variant = "body",
  placeholder,
  isReadOnly = false,
  isChecked = false,
  handleRef,
  onWrite,
  onInput,
  onKeyDown,
}: {
  value: string;
  label: string;
  variant?: InlineTextVariant;
  placeholder?: string;
  isReadOnly?: boolean;
  isChecked?: boolean;
  handleRef?: Ref<RichTextHandle>;
  onWrite: (markdown: string) => void;
  /** Called on each local edit with the plain text; return true if it was handled. */
  onInput?: (plainText: string) => boolean;
  /** Return true when the key was handled; waiting edits are written first. */
  onKeyDown?: (event: KeyboardEvent, info: RichTextKeyInfo) => boolean;
}) {
  // The value last adopted from Jazz or written to it.
  const synced = useRef(value);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const lastWrite = useRef(0);
  const callbacks = useRef({ onWrite, onInput, onKeyDown, placeholder });
  callbacks.current = { onWrite, onInput, onKeyDown, placeholder };
  const editorRef = useRef<Editor | null>(null);
  // A focus request that came before the editor was created.
  const pendingFocus = useRef<"start" | "end" | null>(null);

  const cancelWrite = () => {
    if (timer.current !== null) clearTimeout(timer.current);
    timer.current = null;
  };
  const write = () => {
    const editor = editorRef.current;
    if (!editor) return;
    const markdown = docToMarkdown(editor.getJSON());
    if (markdown === synced.current) return;
    synced.current = markdown;
    lastWrite.current = Date.now();
    callbacks.current.onWrite(markdown);
  };
  const flush = () => {
    if (timer.current === null) return;
    cancelWrite();
    write();
  };

  const editor = useEditor(
    {
      extensions: [
        ...richTextExtensions,
        Placeholder.configure({ placeholder: () => callbacks.current.placeholder ?? "" }),
      ],
      content: markdownToDoc(value),
      editable: !isReadOnly,
      immediatelyRender: false,
      editorProps: {
        attributes: { role: "textbox", "aria-multiline": "true", "aria-label": label },
        handleKeyDown: (view, event) => {
          if (!view.editable) return false;
          const { selection, doc } = view.state;
          const info = {
            isEmpty: doc.textContent.length === 0,
            caretAtStart: selection.empty && selection.from === Selection.atStart(doc).from,
            caretAtEnd: selection.empty && selection.to === Selection.atEnd(doc).to,
          };
          if (event.key === "Enter" && event.shiftKey) {
            // A line break inside the block, kept as a newline in the Markdown.
            view.dispatch(view.state.tr.insertText("\n"));
            return true;
          }
          const handled = callbacks.current.onKeyDown?.(event, info) ?? false;
          if (handled) flush();
          return handled;
        },
      },
      onUpdate: ({ editor, transaction }) => {
        if (!transaction.docChanged || transaction.getMeta(FROM_JAZZ)) return;
        if (callbacks.current.onInput?.(editor.getText())) return;
        if (timer.current !== null) return;
        const wait = lastWrite.current + WRITE_INTERVAL_MS - Date.now();
        if (wait <= 0) return write();
        timer.current = setTimeout(() => {
          timer.current = null;
          write();
        }, wait);
      },
      onBlur: () => flush(),
    },
    [],
  );
  editorRef.current = editor;

  useEffect(() => {
    if (!editor || value === synced.current) return;
    if (timer.current === null) {
      synced.current = value;
      applyMarkdown(editor, value);
      return;
    }
    cancelWrite();
    const next = rebaseText(synced.current, docToMarkdown(editor.getJSON()), value);
    synced.current = next;
    applyMarkdown(editor, next);
    if (next === value) return;
    lastWrite.current = Date.now();
    callbacks.current.onWrite(next);
  }, [editor, value]);

  useEffect(() => {
    if (!editor || !pendingFocus.current) return;
    editor.commands.focus(pendingFocus.current);
    pendingFocus.current = null;
  }, [editor]);

  useEffect(() => {
    editor?.setEditable(!isReadOnly);
  }, [editor, isReadOnly]);

  useEffect(() => {
    editor?.view.dom.setAttribute("aria-label", label);
  }, [editor, label]);

  // Edits still waiting when the block goes away (another page, a kind
  // without text) are written rather than lost.
  useEffect(() => () => flush(), []);

  useImperativeHandle(
    handleRef,
    () => ({
      focus: (at) => {
        if (editor) editor.commands.focus(at);
        else pendingFocus.current = at;
      },
      flush,
      plainText: () => editor?.getText() ?? "",
      takeLeading: (count) => {
        if (!editor) return synced.current;
        cancelWrite();
        const start = Selection.atStart(editor.state.doc).from;
        editor.view.dispatch(editor.state.tr.delete(start, start + count).setMeta(FROM_JAZZ, true));
        synced.current = docToMarkdown(editor.getJSON());
        return synced.current;
      },
    }),
    [editor],
  );

  return (
    <div
      className="bb-inline-text bb-rich-text"
      data-variant={variant}
      data-checked={isChecked ? "true" : undefined}
    >
      <EditorContent editor={editor} />
    </div>
  );
}

// #region rich-text-apply
/** Replace the document with `markdown`, changing only the part that differs. */
function applyMarkdown(editor: Editor, markdown: string) {
  const { state } = editor;
  const next = state.schema.nodeFromJSON(markdownToDoc(markdown));
  const start = state.doc.content.findDiffStart(next.content);
  if (start === null) return;
  let { a: endA, b: endB } = state.doc.content.findDiffEnd(next.content)!;
  const overlap = start - Math.min(endA, endB);
  if (overlap > 0) {
    endA += overlap;
    endB += overlap;
  }
  const tr = state.tr
    .replace(start, endA, next.slice(start, endB))
    .setMeta(FROM_JAZZ, true)
    .setMeta("addToHistory", false);
  editor.view.dispatch(tr);
}
// #endregion rich-text-apply
