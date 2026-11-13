// `<include>` of a code snippet leaves its source reference
// ("../examples/docs/…/snippets.ts#region") in a page's structured data, so
// search would show it as a match. Index the page without those entries.
const INCLUDE_REF = /^(?:\.\.?\/)+[^\s#]+\.[A-Za-z]+(?:#[\w-]+)?$/;

export function isIncludeRef(content: string): boolean {
  return INCLUDE_REF.test(content.trim());
}

export function withoutIncludeRefs<T extends { contents: { content: string }[] }>(data: T): T {
  return { ...data, contents: data.contents.filter((entry) => !isIncludeRef(entry.content)) };
}
