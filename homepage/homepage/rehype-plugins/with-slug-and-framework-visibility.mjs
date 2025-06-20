// @ts-check

import { visit } from "unist-util-visit";
import { processHeadingNode } from "./utils/processing-heading-node.mjs";

/**
 * A rehype plugin that adds IDs to heading elements and handles framework visibility markers.
 * It also tracks which frameworks each heading should be visible for.
 * A modified version of rehype-slug.
 *
 * @returns {import('unified').Plugin<[], import('hast').Root>} A rehype plugin
 */
export function withSlugAndHeadingsFrameworkVisibility() {
  return function (tree, vfile) {
    vfile.data.headingsFrameworkVisibility =
      vfile.data.headingsFrameworkVisibility || {};

    visit(tree, "element", function (node) {
      const id = processHeadingNode(
        node,
        vfile.data.headingsFrameworkVisibility,
      );
      if (id) {
        node.properties.id = id;
      }
    });
  };
}
