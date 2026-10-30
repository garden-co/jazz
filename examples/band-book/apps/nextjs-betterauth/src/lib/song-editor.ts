import { baseKeymap, setBlockType, toggleMark, wrapIn } from "prosemirror-commands";
import { history, redo, undo } from "prosemirror-history";
import {
  inputRules,
  textblockTypeInputRule,
  wrappingInputRule,
  type InputRule,
} from "prosemirror-inputrules";
import { keymap } from "prosemirror-keymap";
import { defaultMarkdownParser, schema } from "prosemirror-markdown";
import type { MarkType, Node, NodeType } from "prosemirror-model";
import { liftListItem, sinkListItem, splitListItem, wrapInList } from "prosemirror-schema-list";
import type { Command, EditorState, Plugin } from "prosemirror-state";

/**
 * The song sheet editor's document model: CommonMark, as `prosemirror-markdown`
 * defines it, so what people edit is exactly what the Markdown column can hold.
 */
export const sheetSchema = schema;

// #region rich-text-parse
/** Reads a song sheet's stored Markdown into an editor document. */
export function parseSheet(markdown: string): Node {
  return defaultMarkdownParser.parse(markdown);
}
// #endregion rich-text-parse

const { nodes, marks } = sheetSchema;

/** Markdown shortcuts as you type: `# `, `- `, `1. `, `> ` and a code fence. */
function shortcuts(): Plugin {
  const rules: InputRule[] = [
    textblockTypeInputRule(/^(#{1,3})\s$/, nodes.heading, (match: RegExpMatchArray) => ({
      level: match[1]!.length,
    })),
    wrappingInputRule(/^\s*([-+*])\s$/, nodes.bullet_list),
    wrappingInputRule(
      /^(\d+)\.\s$/,
      nodes.ordered_list,
      (match: RegExpMatchArray) => ({ order: Number(match[1]) }),
      (match, node) => node.childCount + node.attrs.order === Number(match[1]),
    ),
    wrappingInputRule(/^\s*>\s$/, nodes.blockquote),
    textblockTypeInputRule(/^```$/, nodes.code_block),
  ];
  return inputRules({ rules });
}

/** History, keyboard shortcuts and Markdown shortcuts for the sheet editor. */
export function editingPlugins(): Plugin[] {
  return [
    history(),
    shortcuts(),
    keymap({
      "Mod-z": undo,
      "Shift-Mod-z": redo,
      "Mod-y": redo,
      "Mod-b": toggleMark(marks.strong),
      "Mod-i": toggleMark(marks.em),
      "Mod-e": toggleMark(marks.code),
      Enter: splitListItem(nodes.list_item),
      Tab: sinkListItem(nodes.list_item),
      "Shift-Tab": liftListItem(nodes.list_item),
      "Shift-Enter": (state, dispatch) => {
        dispatch?.(state.tr.replaceSelectionWith(nodes.hard_break.create()).scrollIntoView());
        return true;
      },
    }),
    keymap(baseKeymap),
  ];
}

export type SheetTool = {
  label: string;
  /** Short text shown on the button. */
  short: string;
  run: Command;
  isActive?: (state: EditorState) => boolean;
};

function markActive(type: MarkType) {
  return (state: EditorState) => {
    const { from, $from, to, empty } = state.selection;
    if (empty) return Boolean(type.isInSet(state.storedMarks ?? $from.marks()));
    return state.doc.rangeHasMark(from, to, type);
  };
}

function blockActive(type: NodeType, attrs?: Record<string, unknown>) {
  return (state: EditorState) => {
    const { $from } = state.selection;
    const parent = $from.parent;
    return (
      parent.type === type &&
      Object.entries(attrs ?? {}).every(([key, value]) => parent.attrs[key] === value)
    );
  };
}

/** Wraps the selection in a link, or removes the link it is in. */
export function toggleLink(href: string | null): Command {
  return (state, dispatch) => {
    const { from, to, empty } = state.selection;
    if (empty) return false;
    if (dispatch) {
      const tr = state.tr.removeMark(from, to, marks.link);
      if (href) tr.addMark(from, to, marks.link.create({ href }));
      dispatch(tr);
    }
    return true;
  };
}

/** The formatting toolbar, in display order. */
export const SHEET_TOOLS: SheetTool[][] = [
  [
    {
      label: "Bold",
      short: "B",
      run: toggleMark(marks.strong),
      isActive: markActive(marks.strong),
    },
    { label: "Italic", short: "I", run: toggleMark(marks.em), isActive: markActive(marks.em) },
    { label: "Code", short: "</>", run: toggleMark(marks.code), isActive: markActive(marks.code) },
  ],
  [
    {
      label: "Paragraph",
      short: "¶",
      run: setBlockType(nodes.paragraph),
      isActive: blockActive(nodes.paragraph),
    },
    ...[1, 2, 3].map((level) => ({
      label: `Heading ${level}`,
      short: `H${level}`,
      run: setBlockType(nodes.heading, { level }),
      isActive: blockActive(nodes.heading, { level }),
    })),
  ],
  [
    { label: "Bulleted list", short: "•", run: wrapInList(nodes.bullet_list) },
    { label: "Numbered list", short: "1.", run: wrapInList(nodes.ordered_list) },
    { label: "Quote or lyrics", short: "❝", run: wrapIn(nodes.blockquote) },
    {
      label: "Chord chart (code block)",
      short: "♯",
      run: setBlockType(nodes.code_block),
      isActive: blockActive(nodes.code_block),
    },
    {
      label: "Divider",
      short: "—",
      run: (state, dispatch) => {
        dispatch?.(state.tr.replaceSelectionWith(nodes.horizontal_rule.create()).scrollIntoView());
        return true;
      },
    },
  ],
];
