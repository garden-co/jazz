import { Group } from "jazz-tools";
import { createJazzTestAccount } from "jazz-tools/testing";
const colleagueAccount = await createJazzTestAccount();
const vector: number[] = [];
// #region Basic
import { co, z } from "jazz-tools";

const Embedding = co.vector(384); // Define 384-dimensional embedding

const Document = co.map({
  content: z.string(),
  embedding: Embedding,
});

export const DocumentsList = co.list(Document);
// #endregion

const documents = DocumentsList.create([]);
const createEmbedding: (text: string) => Promise<number[]> = async () => [];

// #region Create
// Generate embeddings (bring your own embeddings model)
const vectorData = await createEmbedding("Text");

const newDocument = Document.create({
  content: "Text",
  embedding: Embedding.create(vectorData),
});

documents.$jazz.push(newDocument);
// #endregion

// #region Ownership
// Create with shared ownership
const teamGroup = Group.create();
teamGroup.addMember(colleagueAccount, "writer");

const teamList = co.vector(384).create(vector, { owner: teamGroup });
// #endregion

const documentsListId = "";

// #region SemanticSearch
// // 1) Load your documents
const allDocuments = await DocumentsList.load(documentsListId, {
  resolve: {
    $each: { embedding: true },
  },
});

// 2) Obtain vector for your search query
const queryEmbedding = await createEmbedding("search query");

// 3) Sort documents by vector similarity
const similarDocuments = documents.$isLoaded ? documents.map((value) => ({
  value,
  similarity: value.embedding.$jazz.cosineSimilarity(queryEmbedding), // [!code ++]
}))
  .sort((a, b) => b.similarity - a.similarity)
  .filter((result) => result.similarity > 0.5) : null;
// #endregion
