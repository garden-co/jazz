"use client";

import { TextCursorIcon } from "lucide-react";
import { userColors } from "./helpers";
import { useEffect, useState } from "react";
import clsx from "clsx";

export function EditorIndicator({ by }: { by: string }) {
  const [state, setState] = useState<"idle" | "moving" | "fade-out">("idle");
  useEffect(() => {
    const timeout1 = setTimeout(() => {
      setState("moving");
    }, 500);
    const timeout2 = setTimeout(() => {
      setState("fade-out");
    }, 1200);
    return () => {
      clearTimeout(timeout1);
      clearTimeout(timeout2);
    };
  }, []);
  return (
    <span
      className={clsx(
        userColors[by as keyof typeof userColors],
        "absolute text-2xl transition-all bg-black w-full pl-2",
      )}
      style={{ opacity: state === "idle" || state === "moving" ? 100 : 0, left: (state === "moving" || state === "fade-out" ? "100%" : "0") }}
    >
      <TextCursorIcon className="relative -top-0.5 -ml-2 -mr-1 inline-block h-7" />
      {by}
    </span>
  );
}
