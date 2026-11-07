"use client";

import { useState } from "react";
import { CodeBlock } from "@astryxdesign/core/CodeBlock";
import { Tab, TabList } from "@astryxdesign/core/TabList";
import { codeLanguage, customTokenizer } from "@/lib/code-tokenizers";

export type CodeFile = { name: string; language: string; code: string };

/** Several source files in one window, one tab per file. */
export function CodeWindow({ files }: { files: CodeFile[] }) {
  const [selected, setSelected] = useState(files[0]?.name ?? "");
  const file = files.find((candidate) => candidate.name === selected) ?? files[0];
  if (!file) return null;
  const lang = codeLanguage(file.language);

  return (
    <div className="home-code-window">
      <TabList value={selected} onChange={setSelected} size="sm" hasDivider>
        {files.map((candidate) => (
          <Tab key={candidate.name} value={candidate.name} label={candidate.name} />
        ))}
      </TabList>
      <CodeBlock
        code={file.code}
        language={lang}
        tokenizer={customTokenizer(lang)}
        // Keep syntax colors when Firefox mounts a code block after client navigation.
        highlightMode="spans"
        hasLanguageLabel={false}
        width="100%"
        className="home-code"
      />
    </div>
  );
}
