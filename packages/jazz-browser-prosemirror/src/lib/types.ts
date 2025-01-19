import { Marks } from "jazz-tools";

// Supported mark types
export const MARK_TYPE_LOOKUP = {
  strong: {
    mark: Marks.Strong,
    tag: "strong",
  },
  em: {
    mark: Marks.Em,
    tag: "em",
  },
} as const;

// Supported node types
export const NODE_TYPE_LOOKUP = {
  paragraph: {
    mark: Marks.Paragraph,
    tag: "paragraph",
  },
} as const;

export type MarkType = keyof typeof MARK_TYPE_LOOKUP;
export type NodeType = keyof typeof NODE_TYPE_LOOKUP;
