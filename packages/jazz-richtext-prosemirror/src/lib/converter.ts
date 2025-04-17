import {
  DOMParser as PMDOMParser,
  DOMSerializer as PMDOMSerializer,
  Node as PMNode,
} from "prosemirror-model";
import { schema } from "prosemirror-schema-basic";

/**
 * Converts HTML content to a ProseMirror document.
 *
 * This function takes a string of HTML content and converts it into a ProseMirror document
 * that can be used in the editor. It uses the basic ProseMirror schema to parse the HTML.
 *
 * @param content - The HTML content to convert
 * @returns A ProseMirror document that can be used in the editor
 *
 * @example
 * ```typescript
 * const html = "<p>Hello <strong>world</strong></p>";
 * const doc = htmlToProseMirror(html);
 * ```
 */
export function htmlToProseMirror(content: string) {
  const doc = new DOMParser().parseFromString(content, "text/html");
  return PMDOMParser.fromSchema(schema).parse(doc);
}

/**
 * Converts a ProseMirror document to HTML string.
 *
 * This function takes a ProseMirror document and converts it back to HTML.
 * It serializes the document's content and removes any unnecessary XML namespaces.
 *
 * @param doc - The ProseMirror document to convert
 * @returns An HTML string representation of the document
 *
 * @example
 * ```typescript
 * const html = proseMirrorToHtml(editorState.doc);
 * ```
 */
export function proseMirrorToHtml(doc: PMNode) {
  return new XMLSerializer()
    .serializeToString(
      PMDOMSerializer.fromSchema(schema).serializeFragment(doc.content),
    )
    .replace(/\sxmlns="[^"]+"/g, "");
}
