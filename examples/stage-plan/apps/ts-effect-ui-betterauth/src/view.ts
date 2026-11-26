import type { Html } from "@effect/ui/html";

/** A labelled form control; the label is its accessible name. */
export const field = <M>(h: Html.H<M>, label: string, control: Html.Element<M>) =>
  h.label.children(label, control);
