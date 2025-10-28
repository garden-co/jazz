import type { CodeBlockAttrs } from "prosekit/extensions/code-block";
import { shikiBundledLanguagesInfo } from "prosekit/extensions/code-block";
import type { ReactNodeViewProps } from "prosekit/react";

export default function CodeBlockView(props: ReactNodeViewProps) {
  const attrs = props.node.attrs as CodeBlockAttrs;
  const language = attrs.language;

  const setLanguage = (language: string) => {
    const attrs: CodeBlockAttrs = { language };
    props.setAttrs(attrs);
  };

  return (
    <>
      <div
        className="relative mx-2 top-3 h-0 select-none overflow-visible text-xs"
        contentEditable={false}
      >
        <select
          className="outline-unset focus:outline-unset relative box-border w-auto cursor-pointer select-none appearance-none rounded border-none bg-transparent px-2 py-1 text-xs transition text-[var(--prosemirror-highlight)] opacity-0 hover:opacity-80 [div[data-node-view-root]:hover_&]:opacity-50 [div[data-node-view-root]:hover_&]:hover:opacity-80"
          onChange={(event) => setLanguage(event.target.value)}
          value={language || ""}
        >
          <option value="">Plain Text</option>
          {shikiBundledLanguagesInfo.map((info) => (
            <option key={info.id} value={info.id}>
              {info.name}
            </option>
          ))}
        </select>
      </div>
      <pre ref={props.contentRef} data-language={language}></pre>
    </>
  );
}
