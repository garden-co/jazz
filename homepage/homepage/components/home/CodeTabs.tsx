"use client";

import { clsx } from "clsx";
import CodeStepAction from "./CodeStepAction.mdx";
import CodeStepCloud from "./CodeStepCloud.mdx";
import CodeStepRender from "./CodeStepRender.mdx";
import CodeStepSchema from "./CodeStepSchema.mdx";

const codeFiles = [
  {
    fileName: "schema.ts",
    component: CodeStepSchema,
  },
  {
    fileName: "main.tsx",
    component: CodeStepCloud,
  },
  {
    fileName: "sendMessage.ts",
    component: CodeStepAction,
  },
  {
    fileName: "ChatScreen.tsx",
    component: CodeStepRender,
  },
];

export function CodeTabs({ className }: { className?: string }) {
  return (
    <div className={clsx("grid gap-4 md:grid-cols-2", className)}>
      {codeFiles.map((file) => {
        const CodeComponent = file.component;
        return (
          <div
            key={file.fileName}
            className="rounded-lg border bg-white ring-4 ring-stone-400/20 dark:bg-stone-925"
          >
            <span className="block border-b px-2 py-2 text-xs font-light text-stone-700 dark:text-stone-300 md:px-3 md:text-sm">
              {file.fileName}
            </span>

            <pre className="whitespace-pre-wrap wrap-break-word p-1 pb-2 text-xs md:text-sm [&_code]:whitespace-pre-wrap [&_code]:wrap-break-word">
              <CodeComponent />
            </pre>
          </div>
        );
      })}
    </div>
  );
}
