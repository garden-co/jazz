import { getMdxWithToc } from "@/lib/docMdxContent";
import {
  DocsOpenGraphImage,
  imageSize,
  imageContentType,
} from "gcmp-design-system/src/app/components/organisms/OpenGraphImage";

export const title = "Quickstart";
export const size = imageSize;
export const contentType = imageContentType;
export const alt = "Quickstart";

export default async function Image({
  params,
}: {
  params: Promise<{ framework: string }>;
}) {
  const { framework } = await params;
  const { tocItems } = await getMdxWithToc(framework, []);

  const title = tocItems[0].value;

  return DocsOpenGraphImage({
    title: title,
    framework,
    contents: tocItems[0].children?.map((child) => child.value) ?? [],
  });
}
