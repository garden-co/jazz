import type { EditorView } from "prosemirror-view";
import { BoldIcon, ItalicIcon, UnderlineIcon } from "lucide-react";
import { chainCommands, exitCode, toggleMark } from "prosemirror-commands";
import { redo, undo } from "prosemirror-history";
import { keymap } from "prosemirror-keymap";
import { Schema } from "prosemirror-model";
import { schema as basicSchema } from "prosemirror-schema-basic";
import { addListNodes } from "prosemirror-schema-list";

export const MARK_TYPES = ["strong", "em", "underline"] as const;
export const TOOLBAR_OFFSET = 45;

export const TOOLBAR_BUTTONS = [
  { icon: BoldIcon, markName: "strong" },
  { icon: ItalicIcon, markName: "em" },
  { icon: UnderlineIcon, markName: "underline" },
] as const;

export interface MenuState {
  show: boolean;
  top: number;
  left: number;
  activeMarks: string[];
}

export const INITIAL_MENU_STATE: MenuState = {
  show: false,
  top: 0,
  left: 0,
  activeMarks: [],
};

export function createEditorSchema() {
  const nodes = addListNodes(
    basicSchema.spec.nodes,
    "paragraph block*",
    "block",
  );

  const marks = basicSchema.spec.marks.append({
    underline: {
      parseDOM: [{ tag: "u" }, { style: "text-decoration=underline" }],
      toDOM() {
        return ["u", 0];
      },
    },
  });

  return new Schema({ nodes, marks });
}

export function createEditorKeymap(schema: Schema, onEnter: () => void) {
  return keymap({
    "Mod-b": toggleMark(schema.marks.strong),
    "Mod-i": toggleMark(schema.marks.em),
    "Mod-u": toggleMark(schema.marks.underline),
    "Mod-z": undo,
    "Mod-y": redo,
    "Shift-Mod-z": redo,
    Enter: () => {
      onEnter();
      return true;
    },
    "Shift-Enter": chainCommands(exitCode, (state, dispatch) => {
      if (!dispatch) return true;

      const { doc, selection } = state;
      const { $from } = selection;

      // Check if document already has hard_break nodes
      let hasHardBreak = false;
      doc.descendants((node) => {
        if (node.type === schema.nodes.hard_break) {
          hasHardBreak = true;
          return false;
        }
      });

      // First shift+enter: split paragraph to create a new one
      // Subsequent: insert hard_break
      // Necessary because ProseMirror can't move the cursor into a space where there's no content.
      const tr = hasHardBreak
        ? state.tr.replaceSelectionWith(schema.nodes.hard_break.create())
        : state.tr.split($from.pos);

      tr.scrollIntoView();
      dispatch(tr);
      return true;
    }),
  });
}

export function getActiveMarks(view: EditorView): string[] {
  const { selection, doc, schema, storedMarks } = view.state;
  const { from, to, $from, empty } = selection;

  return MARK_TYPES.filter((name) => {
    const type = schema.marks[name];
    if (!type) return false;

    return empty
      ? !!type.isInSet(storedMarks || $from.marks())
      : doc.rangeHasMark(from, to, type);
  });
}

export function shouldShowToolbar(view: EditorView): boolean {
  const { selection, doc } = view.state;
  const { from, to } = selection;

  if (from === to) return false;

  const text = doc.textBetween(from, to);
  return text.trim().length > 0;
}

export function getToolbarPosition(view: EditorView) {
  const { selection } = view.state;
  const { from, to } = selection;

  const start = view.coordsAtPos(from);
  const end = view.coordsAtPos(to);

  return {
    top: start.top - TOOLBAR_OFFSET,
    left: (start.left + end.right) / 2,
  };
}
