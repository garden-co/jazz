import { co, z } from "jazz-tools";

export const Embedding = co.vector(384);

export const Document = co.map({
  content: z.string(),
  embedding: Embedding,
});

export const DocumentsList = co.list(Document);
