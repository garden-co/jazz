import { co, z } from "jazz-tools";

const Document = co.map({
  title: z.string(),
  file: co.fileStream(), // Store a document file
});
