import type { Db } from "jazz-tools";
import { app, type Document, type DocumentWithParts } from "../schema.js";

export const PDF_MIME_TYPE = "application/pdf";
const PDF_CHUNK_SIZE_BYTES = 256 * 1024;

export function isPdf(file: File): boolean {
  return file.type === PDF_MIME_TYPE || file.name.toLowerCase().endsWith(".pdf");
}

export function documentTitle(filename: string): string {
  return filename.replace(/\.pdf$/i, "").trim() || filename;
}

export async function createEncryptedPdfDocument(
  db: Db,
  orgId: string,
  file: File,
  uploadedBy: string,
): Promise<Document> {
  const bytes = new Uint8Array(await file.arrayBuffer());
  const partIds: string[] = [];
  const partSizes: number[] = [];

  try {
    for (let offset = 0; offset < bytes.length; offset += PDF_CHUNK_SIZE_BYTES) {
      const chunk = bytes.slice(offset, Math.min(offset + PDF_CHUNK_SIZE_BYTES, bytes.length));
      const { value: part } = db.insert(app.document_parts, {
        orgId,
        data: chunk,
      });
      partIds.push(part.id);
      partSizes.push(chunk.length);
    }

    const { value: document } = db.insert(app.documents, {
      orgId,
      title: documentTitle(file.name),
      filename: file.name,
      mimeType: PDF_MIME_TYPE,
      size: bytes.length,
      createdAt: new Date(file.lastModified || Date.now()),
      uploadedBy,
      partIds,
      partSizes,
    });

    return document;
  } catch (error) {
    for (const partId of partIds) {
      db.delete(app.document_parts, partId);
    }
    throw error;
  }
}

export function documentBlob(document: DocumentWithParts): Blob {
  const parts = document.parts ?? [];
  const chunks = parts.map((part, index) => {
    const bytes = normalizeBytes(part.data);
    const expected = document.partSizes[index];
    if (expected !== undefined && bytes.length !== expected) {
      throw new Error(`PDF part ${index + 1} is incomplete.`);
    }
    return bytes;
  });

  if (chunks.length !== document.partIds.length) {
    throw new Error("PDF is missing encrypted parts.");
  }

  return new Blob(chunks.map(toBlobPart), { type: PDF_MIME_TYPE });
}

export function deleteDocument(db: Db, document: DocumentWithParts): void {
  db.delete(app.documents, document.id);
  for (const part of document.parts ?? []) {
    db.delete(app.document_parts, part.id);
  }
}

function normalizeBytes(value: Uint8Array | ArrayBuffer | number[]): Uint8Array {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  return Uint8Array.from(value);
}

function toBlobPart(bytes: Uint8Array): ArrayBuffer {
  const copy = new Uint8Array(bytes.byteLength);
  copy.set(bytes);
  return copy.buffer;
}
