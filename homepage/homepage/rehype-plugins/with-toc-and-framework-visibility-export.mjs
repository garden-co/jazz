// @ts-check

import { valueToEstree } from "estree-util-value-to-estree";

/**
 * @typedef {import('hast').Root} Root
 * @typedef {import('hast').RootContent} RootContent
 * @typedef {import('vfile').VFile} VFile
 * @typedef {import('unified').Transformer} Transformer
 */

/**
 * A rehype plugin that exports the table of contents and framework visibility data when importing a .mdx file
 * This is a modified version of withTocExport from @stefanprobst/rehype-extract-toc
 *
 * @returns {import('unified').Plugin<[], Root>} A rehype plugin
 */
export function withTocAndFrameworkHeadingsVisibilityExport() {
  return function transformer(tree, vfile) {
    if (vfile.data.toc == null) return;

    // Create an MDX JSX node with the exports
    const mdxjsEsmNode = {
      type: "mdxjsEsm",
      data: {
        estree: {
          type: "Program",
          sourceType: "module",
          body: [
            {
              type: "ExportNamedDeclaration",
              source: null,
              specifiers: [],
              declaration: {
                type: "VariableDeclaration",
                kind: "const",
                declarations: [
                  {
                    type: "VariableDeclarator",
                    id: {
                      type: "Identifier",
                      name: "headingsFrameworkVisibility",
                    },
                    init: valueToEstree(vfile.data.headingsFrameworkVisibility),
                  },
                  {
                    type: "VariableDeclarator",
                    id: { type: "Identifier", name: "tableOfContents" },
                    init: valueToEstree(vfile.data.toc),
                  },
                ],
              },
            },
          ],
        },
      },
    };

    tree.children.unshift(mdxjsEsmNode);
  };
}
