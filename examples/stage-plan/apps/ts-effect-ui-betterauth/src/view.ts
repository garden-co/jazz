import type { View } from "@effect/ui";
import type { Html } from "@effect/ui/html";

/** A labelled form control; the label is its accessible name. */
export const field = <M>(h: Html.H<M>, label: string, control: View.Content<M>) =>
  h.label.children(label, control);
