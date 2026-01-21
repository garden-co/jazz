import { co, z } from "jazz-tools";

export const Meta = co.map({
  tag: z.string(),
});

export const Note = co.map({
  text: co.plainText(),
  meta: Meta,
});

export const Attachment = co.map({
  name: z.string(),
  file: co.fileStream(),
});

export const Document = co.map({
  title: z.string(),
  attachments: co.list(Attachment),
});

