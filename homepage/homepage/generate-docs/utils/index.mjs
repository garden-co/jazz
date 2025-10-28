import path from "path";
import fs from "fs/promises";

// Helper functions
export function cleanDescription(description) {
  if (!description) return "";
  return (
    description
      .map((part) => part.text)
      .join(" ")
      .trim()
      // Remove code blocks
      .replace(/```[\s\S]*?```/g, "")
      // Remove line breaks and extra spaces
      .replace(/\s+/g, " ")
      // Clean up backticks
      .replace(/\`/g, "'")
  );
}

export async function writeDocsFile(filename, content) {
  await fs.writeFile(
    path.join(process.cwd(), "public", filename),
    content,
    "utf8",
  );
  console.log(`Documentation generated at 'public/${filename}'`);
}
