import {
  DOMParser as PMDOMParser,
  DOMSerializer as PMDOMSerializer,
} from "prosemirror-model";
import { schema } from "prosemirror-schema-basic";

/**
 * Creates a ProseMirror document from a string of HTML content
 */
export function htmlToProseMirror(content: string) {
  const doc = new DOMParser().parseFromString(content, "text/html");
  return PMDOMParser.fromSchema(schema).parse(doc);
}

/**
 * Converts a ProseMirror document to HTML string
 */
export function proseMirrorToHtml(doc: any) {
  return new XMLSerializer()
    .serializeToString(
      PMDOMSerializer.fromSchema(schema).serializeFragment(doc.content),
    )
    .replace(/\sxmlns="[^"]+"/g, "");
}
