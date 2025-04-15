import { DocsLink } from "@/components/docs/DocsLink";
import type { MDXComponents } from "mdx/types";
import { Heading } from "./components/docs/DocHeading";

export function useMDXComponents(components: MDXComponents): MDXComponents {
  return {
    a: (props) => <DocsLink {...props} />,
    h2: (props) => <Heading tag="h2" {...props} />,
    h3: (props) => <Heading tag="h3" {...props} />,
    h4: (props) => <Heading tag="h4" {...props} />,
    h5: (props) => <Heading tag="h5" {...props} />,
    h6: (props) => <Heading tag="h6" {...props} />,
    ...components,
    CodeWithInterpolation: ({
      highlightedCode,
    }: { highlightedCode: string }) => {
      return <div dangerouslySetInnerHTML={{ __html: highlightedCode }} />;
    },
  } satisfies MDXComponents;
}

export function InterpolateInCode(replace: { [key: string]: string }) {
  return {
    CodeWithInterpolation: ({
      highlightedCode,
    }: { highlightedCode: string }) => {
      const newHighlightedCode = Object.entries(replace).reduce(
        (acc, [key, value]) => {
          return acc.replaceAll(key, value);
        },
        highlightedCode,
      );

      return <div dangerouslySetInnerHTML={{ __html: newHighlightedCode }} />;
    },
  };
}
