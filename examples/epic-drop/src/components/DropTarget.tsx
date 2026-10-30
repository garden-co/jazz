import * as React from "react";
import { isDroppable, readDrop, type DropPayload } from "../drag.js";

interface DropTargetProps {
  children: React.ReactNode;
  isDisabled?: boolean;
  onDrop: (payload: DropPayload) => void;
}

/** Marks its children as a place to drop files or app items, and highlights on hover. */
export function DropTarget({ children, isDisabled, onDrop }: DropTargetProps) {
  const [isOver, setIsOver] = React.useState(false);
  if (isDisabled) return <span className="drop-target">{children}</span>;
  return (
    <span
      className="drop-target"
      data-over={isOver || undefined}
      onDragOver={(event) => {
        if (!isDroppable(event)) return;
        event.preventDefault();
        event.dataTransfer.dropEffect = event.dataTransfer.types.includes("Files")
          ? "copy"
          : "move";
        setIsOver(true);
      }}
      onDragLeave={() => setIsOver(false)}
      onDrop={(event) => {
        event.preventDefault();
        event.stopPropagation();
        setIsOver(false);
        const payload = readDrop(event);
        if (payload) onDrop(payload);
      }}
    >
      {children}
    </span>
  );
}
