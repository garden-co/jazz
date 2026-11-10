import { h } from "./dom";

/**
 * The little Markdown the agent writes (paragraphs, `-` lists, **bold**),
 * built as DOM nodes so model output is never parsed as HTML.
 */
export function markdown(text: string): HTMLElement {
  const root = h("div", { class: "markdown" });
  let list: HTMLUListElement | undefined;
  for (const block of text.split(/\n{2,}/)) {
    for (const line of block.split("\n")) {
      const item = /^\s*[-*]\s+(.*)$/.exec(line);
      if (item) {
        list ??= root.appendChild(h("ul"));
        list.append(h("li", {}, ...inline(item[1]!)));
        continue;
      }
      list = undefined;
      if (line.trim()) root.append(h("p", {}, ...inline(line)));
    }
    list = undefined;
  }
  return root;
}

function inline(text: string): (string | Node)[] {
  return text
    .split(/(\*\*[^*]+\*\*)/)
    .filter(Boolean)
    .map((part) => (/^\*\*[^*]+\*\*$/.test(part) ? h("strong", {}, part.slice(2, -2)) : part));
}
