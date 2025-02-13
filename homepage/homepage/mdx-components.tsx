import type { MDXComponents } from "mdx/types";

export function useMDXComponents(components: MDXComponents): MDXComponents {
  return {
    ...components,
    CodeWithInterpolation: ({
      highlightedCode,
    }: { highlightedCode: string }) => {
      return <div dangerouslySetInnerHTML={{ __html: highlightedCode }} />;
    },
  };
}

export function InterpolateInCode(replace: { [key: string]: string }) {
  return {
    CodeWithInterpolation: ({
      highlightedCode,
    }: { highlightedCode: string }) => {
      const newHighlightedCode = Object.entries(replace).reduce(
        (acc, [key, value]) => {
          return acc.replaceAll(
            key.replaceAll("$", "&#36;").replaceAll("_", "&#95;"),
            value,
          );
        },
        highlightedCode,
      );
      return <div dangerouslySetInnerHTML={{ __html: newHighlightedCode }} />;
    },
  };
}
