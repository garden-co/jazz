import {
  Deserializer,
  FileRegistry,
  ProjectReflection,
} from "typedoc";


const docs = {};

export async function requestProject(
  packageName: keyof typeof docs,
): Promise<ProjectReflection> {
  // Check if TypeDoc data exists for this package
  if (!docs[packageName]) {
    // Throw an error when TypeDoc data is missing to prevent prerender issues
    throw new Error(`TypeDoc data not found for package: ${packageName}. Please run 'pnpm run generate:docs' to generate API documentation.`);
  }

  const deserializer = new Deserializer({} as any);
  return deserializer.reviveProject(packageName, docs[packageName], {
    projectRoot: "/",
    registry: new FileRegistry(),
  });
}
