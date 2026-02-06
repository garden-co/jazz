import type { EditorView } from "prosemirror-view";
import { toggleMark } from "prosemirror-commands";
import { Button } from "@/components/ui/button";
import { ButtonGroup } from "@/components/ui/button-group";
import { type MenuState, TOOLBAR_BUTTONS } from "./utils";

interface ToolbarButtonProps {
  icon: React.ComponentType;
  markName: string;
  isActive: boolean;
  onClick: () => void;
}

function ToolbarButton({
  icon: Icon,
  markName,
  isActive,
  onClick,
}: ToolbarButtonProps) {
  return (
    <Button
      variant={isActive ? "default" : "secondary"}
      onClick={onClick}
      aria-label={markName}
    >
      <Icon />
    </Button>
  );
}

interface FloatingToolbarProps {
  view: EditorView | null;
  menuState: MenuState;
}

export function FloatingToolbar({ view, menuState }: FloatingToolbarProps) {
  if (!menuState.show || !view) return null;

  const handleToggle = (markName: string) => {
    const mark = view.state.schema.marks[markName];
    if (mark) {
      toggleMark(mark)(view.state, view.dispatch);
      view.focus();
    }
  };

  return (
    <div
      role="toolbar"
      style={{
        position: "fixed",
        top: menuState.top,
        left: menuState.left,
        transform: "translateX(-50%)",
      }}
      className="z-50 rounded-sm border bg-background shadow-md overflow-hidden"
      onMouseDown={(evt) => evt.preventDefault()}
    >
      <ButtonGroup>
        {TOOLBAR_BUTTONS.map(({ icon, markName }) => (
          <ToolbarButton
            key={markName}
            icon={icon}
            markName={markName}
            isActive={menuState.activeMarks.includes(markName)}
            onClick={() => handleToggle(markName)}
          />
        ))}
      </ButtonGroup>
    </div>
  );
}
