import { mdxToMd } from "@/generate-docs/utils/mdx-processor.mjs";
import fs from "fs";
import path from "path";
import { NextRequest, NextResponse } from "next/server";

type Params = {
  framework: string;
  slug?: string[];
};

const DOCS_DIR = path.join(process.cwd(), "content", "docs");

/**
 * Get the file path for an MDX file given framework and slug
 */
function getMdxFilePath(framework: string, slug?: string[]): string | null {
  const slugPath = slug?.join("/");

  // First try framework-specific MDX
  if (slugPath) {
    const frameworkPath = path.join(DOCS_DIR, slugPath, `${framework}.mdx`);
    if (fs.existsSync(frameworkPath)) {
      return frameworkPath;
    }
  }

  // Fallback to generic MDX
  if (slugPath) {
    const genericPath = path.join(DOCS_DIR, `${slugPath}.mdx`);
    if (fs.existsSync(genericPath)) {
      return genericPath;
    }
  }

  // Top-level index fallback
  const indexPath = path.join(DOCS_DIR, "index.mdx");
  if (fs.existsSync(indexPath)) {
    return indexPath;
  }

  return null;
}

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<Params> }
) {
  const awaitedParams = await params;
  const framework = awaitedParams.framework;
  const slug = awaitedParams.slug ?? [];
  
  // Check if this is a .md request by checking the URL pathname
  const url = new URL(request.url);
  if (!url.pathname.endsWith(".md")) {
    // Not a .md request - return 404 so page.tsx can handle it
    return new NextResponse(null, { status: 404 });
  }

  // Remove .md from the last slug segment
  let cleanSlug = slug;
  if (slug.length > 0) {
    const lastSegment = slug[slug.length - 1];
    if (lastSegment?.endsWith(".md")) {
      cleanSlug = [...slug.slice(0, -1), lastSegment.replace(/\.md$/, "")];
    }
  }

  const filePath = getMdxFilePath(framework, cleanSlug);
  if (!filePath) {
    return new NextResponse("Document not found", { status: 404 });
  }

  try {
    const markdown = await mdxToMd(filePath, framework);
    return new NextResponse(markdown, {
      headers: {
        "Content-Type": "text/markdown; charset=utf-8",
      },
    });
  } catch (error) {
    console.error("Error converting MDX to Markdown:", error);
    return new NextResponse("Error generating markdown", { status: 500 });
  }
}
