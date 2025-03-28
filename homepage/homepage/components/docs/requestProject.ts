import { Deserializer, FileRegistry, JSONOutput, ProjectReflection } from "typedoc";

import JazzBrowserMediaImagesDocs from "../../typedoc/jazz-browser-media-images.json";
import JazzBrowserDocs from "../../typedoc/jazz-browser.json";
import JazzNodejsDocs from "../../typedoc/jazz-nodejs.json";
import JazzReactDocs from "../../typedoc/jazz-react.json";
import JazzToolsDocs from "../../typedoc/jazz-tools.json";

const docs = {
  "jazz-tools": JazzToolsDocs as unknown as JSONOutput.ProjectReflection,
  "jazz-react": JazzReactDocs as unknown as JSONOutput.ProjectReflection,
  "jazz-browser": JazzBrowserDocs as unknown as JSONOutput.ProjectReflection,
  "jazz-browser-media-images":
    JazzBrowserMediaImagesDocs as unknown as JSONOutput.ProjectReflection,
  "jazz-nodejs": JazzNodejsDocs as unknown as JSONOutput.ProjectReflection,
};

export async function requestProject(
  packageName: keyof typeof docs,
): Promise<ProjectReflection> {
  const deserializer = new Deserializer({} as any);
  return deserializer.reviveProject(packageName, docs[packageName], {
    projectRoot: "/",
    registry: new FileRegistry(),
});
}
