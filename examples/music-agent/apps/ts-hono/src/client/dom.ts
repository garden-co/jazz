type Child = Node | string | null | undefined | false;
type Props = Record<string, unknown> & {
  class?: string;
  on?: Record<string, (event: Event) => void>;
};

/** A tiny element builder: attributes, event listeners and children. */
export function h<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  props: Props = {},
  ...children: (Child | Child[])[]
): HTMLElementTagNameMap[K] {
  const element = document.createElement(tag);
  const { on, ...attributes } = props;
  for (const [name, value] of Object.entries(attributes)) {
    if (value === undefined || value === null || value === false) continue;
    if (name in element && typeof value !== "string") (element as any)[name] = value;
    else element.setAttribute(name, value === true ? "" : String(value));
  }
  for (const [event, listener] of Object.entries(on ?? {}))
    element.addEventListener(event, listener);
  element.append(...children.flat().filter((child): child is Node | string => !!child));
  return element;
}

/** A labelled text input; the label is its accessible name. */
export function field(label: string, input: HTMLInputElement | HTMLTextAreaElement) {
  return h("label", {}, label, input);
}
