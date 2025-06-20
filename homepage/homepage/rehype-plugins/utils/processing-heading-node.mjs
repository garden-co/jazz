// @ts-check

import { headingRank } from "hast-util-heading-rank";
import { toString } from "hast-util-to-string";
import { generateHeaderId } from "./header-ids.mjs";

const FRAMEWORK_MARKER_REGEX = /\s*\[\!framework=([a-zA-Z0-9,_-]+)\]\s*$/;

/**
 * @typedef {import('hast').Element} Element
 * @typedef {import('hast').Text} Text
 * @typedef {Element | Text} HastNode
 */

/**
 * Processes a heading node to extract framework visibility and generate an ID
 * @param {Element} node - The heading node to process
 * @param {Record<string, string[]>} [frameworkVisibility] - Object to store framework visibility info
 * @returns {string | undefined} The generated header ID if processed, undefined otherwise
 */
export function processHeadingNode(node, frameworkVisibility = {}) {
  if (!headingRank(node) || node.properties?.id) {
    return undefined;
  }

  const headerText = toString(node);
  const lastChild = node.children?.[node.children.length - 1];

  if (lastChild?.type === "text") {
    const frameworkMatch = lastChild.value.match(FRAMEWORK_MARKER_REGEX);

    if (frameworkMatch) {
      const frameworks = frameworkMatch[1];
      lastChild.value = lastChild.value.replace(FRAMEWORK_MARKER_REGEX, "");
      const id = generateHeaderId(headerText);
      frameworkVisibility[id] = frameworks.split(",");
      return id;
    }
  }

  return generateHeaderId(headerText);
}
