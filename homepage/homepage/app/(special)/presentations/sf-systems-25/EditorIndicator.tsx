"use client";

import { TextCursorIcon } from "lucide-react";
import { userColors } from "./helpers";
import { useEffect, useState } from "react";
import clsx from "clsx";

export function EditorIndicator({ by }: { by: string }) {
  const [opacity, setOpacity] = useState(100);
  const [left, setLeft] = useState(-100);
  useEffect(() => {
    const timeout = setTimeout(() => {
      setOpacity(0);
    }, 1000);
    setLeft(0);
    return () => clearTimeout(timeout);
  }, []);
  return (
    <span
      className={clsx(
        userColors[by as keyof typeof userColors],
        "text-xs transition-all relative",
      )}
      style={{ opacity, left: left + "px" }}
    >
      <TextCursorIcon className="relative -top-0.5 -ml-2 -mr-2 inline-block h-4" />
      {by}
    </span>
  );
}
