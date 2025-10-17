"use client";

import { clsx } from "clsx";
import { useState } from "react";
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

function Code({ className }: { className?: string }) {
  const [activeTab, setActiveTab] = useState(0);
  const activeFile = codeFiles[activeTab];
  const CodeComponent = activeFile.component;

  return (
    <div
      className={clsx(
        "rounded-lg border bg-white ring-4 ring-stone-400/20 dark:bg-stone-925",
        className,
      )}
    >
      <div className="flex gap-1 border-b overflow-x-auto [&::-webkit-scrollbar]:hidden [-ms-overflow-style:none] [scrollbar-width:none]">
        {codeFiles.map((file, index) => (
          <button
            type="button"
            key={file.fileName}
            onClick={() => setActiveTab(index)}
            className={clsx(
              "text-xs md:text-sm py-2 px-2 md:px-3 transition-colors whitespace-nowrap",
              activeTab === index
                ? "border-b-2 border-blue-400 text-blue-400"
                : "text-stone-600 dark:text-stone-400 hover:text-stone-900 dark:hover:text-stone-100",
            )}
          >
            {file.fileName}
          </button>
        ))}
      </div>

      <pre className="text-xs md:text-sm p-1 pb-2 whitespace-pre-wrap break-words [&_code]:whitespace-pre-wrap [&_code]:break-words">
        <CodeComponent />
      </pre>
    </div>
  );
}

export function CodeTabs({ className }: { className?: string }) {
  return <Code className={className} />;
}
