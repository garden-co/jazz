"use client";

import { clsx } from "clsx";
import { useEffect, useId, useRef, useState } from "react";
import { Icon } from "../atoms/Icon";

// TODO: add tabs feature, and remove CodeExampleTabs

export function CopyButton({
  code,
  size,
  className,
}: { code: string; size: "md" | "lg"; className?: string }) {
  const [copyCount, setCopyCount] = useState(0);
  const copied = copyCount > 0;

  useEffect(() => {
    if (copyCount > 0) {
      const timeout = setTimeout(() => setCopyCount(0), 1000);
      return () => {
        clearTimeout(timeout);
      };
    }
  }, [copyCount]);

  return (
    <button
      type="button"
      className={clsx(
        className,
        "group/button absolute overflow-hidden rounded text-2xs font-medium md:opacity-0 backdrop-blur transition md:focus:opacity-100 group-hover:opacity-100",
        copied
          ? "bg-emerald-400/10 ring-1 ring-inset ring-emerald-400/20"
          : "bg-white/5 hover:bg-white/7.5 dark:bg-white/2.5 dark:hover:bg-white/5",
        size == "md"
          ? "right-[8.5px] top-[8.5px] py-[2px] pl-1 pr-2"
          : "right-2 top-2 py-1 pl-2 pr-3 ",
      )}
      onClick={() => {
        window.navigator.clipboard.writeText(code).then(() => {
          setCopyCount((count) => count + 1);
        });
      }}
    >
      <span
        aria-hidden={copied}
        className={clsx(
          "pointer-events-none flex items-center gap-1 text-stone-500 dark:text-stone-400 transition duration-300 group-hover/button:text-stone-600 dark:group-hover/button:text-stone-300",
          copied && "-translate-y-1.5 opacity-0",
        )}
      >
        <Icon
          name="copy"
          size="xs"
          className={clsx(
            size === "md" ? "size-3" : "size-4",
            "stroke-stone-500 transition-colors group-hover/button:stroke-stone-600 dark:group-hover/button:stroke-stone-400",
          )}
        />
        Copy
      </span>
      <span
        aria-hidden={!copied}
        className={clsx(
          "pointer-events-none absolute inset-0 flex items-center justify-center text-emerald-600 transition duration-300 dark:text-emerald-400",
          !copied && "translate-y-1.5 opacity-0",
        )}
      >
        Copied!
      </span>
    </button>
  );
}

export function CodeGroup({
  children,
  size = "md",
  className,
}: {
  children?: React.ReactNode;
  text?: string;
  size?: "md" | "lg";
  className?: string;
}) {
  const textRef = useRef<HTMLPreElement | null>(null);
  const [code, setCode] = useState<string>();
  useEffect(() => {
    if (textRef.current) {
      setCode(textRef.current.innerText);
    }
  }, [children]);

  return (
    <div className={clsx(className, "not-prose group relative")}>
      <pre
        className={clsx(
          "h-full overflow-x-auto",
          "border rounded-md p-0 bg-stone-50 dark:bg-stone-925",
          "text-black dark:text-white",
          {
            "text-sm": size === "md",
          },
        )}
        ref={textRef}
      >
        {children}
      </pre>

      {code ? <CopyButton size={size} code={code} /> : <></>}
    </div>
  );
}
