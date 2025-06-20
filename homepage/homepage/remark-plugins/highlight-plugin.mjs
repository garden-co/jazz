import { transformerNotationDiff } from "@shikijs/transformers";
import { transformerTwoslash } from "@shikijs/twoslash";
import { createHighlighter } from "shiki";
import { SKIP, visit } from "unist-util-visit";
import { jazzDark } from "../themes/jazzDark.mjs";
import { jazzLight } from "../themes/jazzLight.mjs";

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
  ],
  themes: [jazzLight, jazzDark],
});

/**
 * A remark plugin that highlights code blocks
 * @returns {import('unified').Plugin<[], import('mdast').Root>} A remark plugin
 */
export function highlightPlugin() {
  return async function transformer(tree) {
    const highlighter = await highlighterPromise;
    visit(tree, "code", visitor);

    function visitor(node) {
      /** @type {any} */
      let error = null;

      const html = highlighter.codeToHtml(node.value, {
        lang: node.lang,
        meta: { __raw: node.lang + " " + node.meta },
        themes: {
          light: "jazz-light",
          dark: "jazz-dark",
        },

        transformers: [
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

      node.type = "html";
      node.value = error
        ? `<div style="color: red; background: #fee; padding: 8px; border: 1px solid #fcc; margin: 8px 0;">
            <strong>Twoslash Error:</strong> ${error.description || error.message || "Unknown error"}
            ${error.recommendation ? `<div>${error.recommendation}</div>` : ""}
          </div>` + html
        : html;
      node.children = [];
      return SKIP;
    }
  };
}
