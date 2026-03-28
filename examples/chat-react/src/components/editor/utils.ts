import { Schema } from "prosemirror-model";
import { nodes as basicNodes, marks as basicMarks } from "prosemirror-schema-basic";
import { addListNodes } from "prosemirror-schema-list";
import { keymap } from "prosemirror-keymap";
import { baseKeymap, toggleMark } from "prosemirror-commands";
import { history, undo, redo } from "prosemirror-history";
import type { MarkType } from "prosemirror-model";
import type { EditorState, Command } from "prosemirror-state";

const { doc, paragraph, text, hard_break, horizontal_rule, blockquote, heading } = basicNodes;
const { em, strong, code, link } = basicMarks;

export const schema = new Schema({
  nodes: addListNodes(
    new Schema({
      nodes: { doc, paragraph, text, hard_break, horizontal_rule, blockquote, heading },
      marks: { em, strong, code, link },
    }).spec.nodes,
    "paragraph block*",
    "block",
  ),
  marks: { em, strong, code, link },
});

export function editorKeymap(onSend: () => void) {
  return keymap({
    "Mod-b": toggleMark(schema.marks.strong),
    "Mod-i": toggleMark(schema.marks.em),
    "Mod-`": toggleMark(schema.marks.code),
    "Mod-z": undo,
    "Mod-y": redo,
    "Mod-Shift-z": redo,
    Enter: (_state, _dispatch, view) => {
      if (view) onSend();
      return true;
    },
    "Shift-Enter": baseKeymap.Enter,
  });
}

export function historyPlugins() {
  return [history(), keymap({ "Mod-z": undo, "Mod-y": redo })];
}

export function isMarkActive(state: EditorState, markType: MarkType): boolean {
  const { from, $from, to, empty } = state.selection;
  if (empty) {
    return !!markType.isInSet(state.storedMarks || $from.marks());
  }
  return state.doc.rangeHasMark(from, to, markType);
}

export function toggleMarkCommand(markType: MarkType): Command {
  return toggleMark(markType);
}
