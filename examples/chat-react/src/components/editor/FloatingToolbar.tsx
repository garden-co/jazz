import { BoldIcon, ItalicIcon, CodeIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { ButtonGroup } from "@/components/ui/button-group";
import { toggleMarkCommand, isMarkActive, schema } from "./utils";
import type { EditorView } from "prosemirror-view";
import type { FloatingToolbarState } from "./floatingToolbarPlugin";

interface FloatingToolbarProps {
  state: FloatingToolbarState;
  view: EditorView;
}

export function FloatingToolbar({ state, view }: FloatingToolbarProps) {
  if (!state.visible) return null;

  const editorState = view.state;

  const runMark = (markName: "strong" | "em" | "code") => {
    const mark = schema.marks[markName];
    toggleMarkCommand(mark)(editorState, view.dispatch, view);
    view.focus();
  };

  return (
    <div className="absolute z-50 -translate-x-1/2" style={{ top: state.top, left: state.left }}>
      <ButtonGroup>
        <Button
          variant={isMarkActive(editorState, schema.marks.strong) ? "default" : "ghost"}
          size="icon-xs"
          onMouseDown={(e) => {
            e.preventDefault();
            runMark("strong");
          }}
          title="Bold (Cmd+B)"
        >
          <BoldIcon />
        </Button>
        <Button
          variant={isMarkActive(editorState, schema.marks.em) ? "default" : "ghost"}
          size="icon-xs"
          onMouseDown={(e) => {
            e.preventDefault();
            runMark("em");
          }}
          title="Italic (Cmd+I)"
        >
          <ItalicIcon />
        </Button>
        <Button
          variant={isMarkActive(editorState, schema.marks.code) ? "default" : "ghost"}
          size="icon-xs"
          onMouseDown={(e) => {
            e.preventDefault();
            runMark("code");
          }}
          title="Code (Cmd+`)"
        >
          <CodeIcon />
        </Button>
      </ButtonGroup>
    </div>
  );
}
