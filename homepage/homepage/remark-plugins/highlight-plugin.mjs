import { visit, SKIP } from "unist-util-visit";
import { createHighlighter } from "shiki";
import { transformerNotationDiff } from "@shikijs/transformers";
import { transformerTwoslash } from "@shikijs/twoslash";
import { jazzDark } from "../themes/jazzDark.mjs";
import { jazzLight } from "../themes/jazzLight.mjs";
import {
  getCachedResult,
  setCachedResult,
  createCacheKey,
} from "./utils/cache.mjs";

const highlighterPromise = createHighlighter({
  langs: [
    "typescript",
    "bash",
    "tsx",
    "json",
    "ruby",
    "groovy",
    "svelte",
    "vue",
    "diff",
  ],
  themes: [jazzLight, jazzDark],
});

/**
 * Custom Shiki transformer that applies diff styling based on line-range metadata.
 * Reads 'add' and 'del' line numbers from meta and applies appropriate classes.
 */
function transformerDiffFromMeta() {
  return {
    name: "diff-from-meta",
    line(node, line) {
      const add = this.options.meta?.add;
      const del = this.options.meta?.del;

      if (add && add.includes(line)) {
        this.addClassToHast(node, "diff");
        this.addClassToHast(node, "add");
      }
      if (del && del.includes(line)) {
        this.addClassToHast(node, "diff");
        this.addClassToHast(node, "remove");
      }
    },
  };
}

/**
 * A remark plugin that highlights code blocks
 * @returns {import('unified').Plugin<[], import('mdast').Root>} A remark plugin
 */
export function highlightPlugin() {
  return async function transformer(tree) {
    const highlighter = await highlighterPromise;

    // First pass: collect all code blocks
    const codeBlocks = [];
    visit(tree, "code", (node) => {
      codeBlocks.push(node);
    });

    // Second pass: process each code block with caching
    for (const node of codeBlocks) {
      // Create cache key based on code content, language, and meta
      const cacheKey = createCacheKey(node.value, node.lang, node.meta);

      // Try to get cached result first
      const cachedResult = await getCachedResult(cacheKey);
      if (cachedResult) {
        node.type = "html";
        node.value = cachedResult.html;
        node.children = [];
        continue;
      }

      // Cache miss - process the code block
      /** @type {any} */
      let error = null;

      // Extract diff line numbers from hProperties
      const add = node.data?.hProperties?.add?.split(",").map(Number) || [];
      const del = node.data?.hProperties?.del?.split(",").map(Number) || [];

      const html = highlighter.codeToHtml(node.value, {
        lang: node.lang,
        meta: { __raw: node.lang + " " + node.meta, add, del },
        themes: {
          light: "jazz-light",
          dark: "jazz-dark",
        },

        transformers: [
          transformerDiffFromMeta(),
          transformerTwoslash({
            explicitTrigger: true,
            throws: process.env.NODE_ENV === "production",
            onTwoslashError: (e, code) => {
              if (process.env.NODE_ENV === "production") {
                // Re-throw to actually fail the build in production
                throw e;
              }

              // Type guard to check if error has the expected properties
              const errorObj = e && typeof e === "object" ? e : {};
              const description =
                "description" in errorObj
                  ? errorObj.description
                  : "Unknown error";
              const recommendation =
                "recommendation" in errorObj
                  ? errorObj.recommendation
                  : "No recommendation available";

              console.error("\nTwoslash error: ");
              console.log(description);
              console.log(recommendation);
              console.log("\nCode: \n```\n" + code + "\n```");

              // In development, store the error to show inline
              error = /** @type {any} */ (e);
            },
          }),
          transformerNotationDiff(),
        ],
      });

      const finalHtml = error
        ? `<div style="color: red; background: #fee; padding: 8px; border: 1px solid #fcc; margin: 8px 0;">
            <strong>Twoslash Error:</strong> ${error.description || error.message || "Unknown error"}
            ${error.recommendation ? `<div>${error.recommendation}</div>` : ""}
          </div>` + html
        : html;

      // Cache the result (only cache successful results, not errors)
      if (!error) {
        await setCachedResult(cacheKey, { html: finalHtml });
      }

      node.type = "html";
      node.value = finalHtml;
      node.children = [];
    }
  };
}
