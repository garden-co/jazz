// @ts-check

import { fromMarkdown } from 'mdast-util-from-markdown';
import { mdxjs } from 'micromark-extension-mdxjs';
import { mdxFromMarkdown } from 'mdast-util-mdx';
import { visit } from 'unist-util-visit';
import { SKIP } from 'unist-util-visit';

/**
 * A remark plugin that converts HTML nodes to JSX nodes
 * @returns {import('unified').Plugin<[], import('mdast').Root>} A remark plugin
 */
export function remarkHtmlToJsx() {
  return (tree) => {
    visit(tree, 'html', (node, index, parent) => {
      const escapedHtml = JSON.stringify(node.value);
      const jsx = `<CodeWithInterpolation highlightedCode={${escapedHtml}}/>`;
      const rawHtmlNode = fromMarkdown(jsx, {
        extensions: [mdxjs()],
        mdastExtensions: [mdxFromMarkdown()],
      }).children[0];

      Object.assign(node, rawHtmlNode);

      return SKIP;
    });
  };
}
