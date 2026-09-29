"use client";

import { CodeBlock } from "@astryxdesign/core/CodeBlock";
import { codeLanguage, customTokenizer } from "@/lib/code-tokenizers";

/** A titled, highlighted code sample; a client component for the tokenizer. */
export function HomeCode({
  code,
  language,
  title,
}: {
  code: string;
  language: string;
  title: string;
}) {
  const lang = codeLanguage(language);
  return (
    <CodeBlock
      code={code}
      language={lang}
      title={title}
      tokenizer={customTokenizer(lang)}
      hasLanguageLabel={false}
      width="100%"
      className="home-code"
    />
  );
}
