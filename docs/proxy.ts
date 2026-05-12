import { isMarkdownPreferred } from "fumadocs-core/negotiation";
import { type NextRequest, NextResponse } from "next/server";

function rewriteToMarkdown(request: NextRequest, slugPath: string) {
  const url = request.nextUrl.clone();
  url.pathname = `/llms.mdx/docs${slugPath}`;
  const res = NextResponse.rewrite(url);
  res.headers.set("Vary", "Accept");
  return res;
}

export function proxy(request: NextRequest) {
  const { pathname } = request.nextUrl;

  if (pathname === "/docs.md" || (pathname.startsWith("/docs/") && pathname.endsWith(".md"))) {
    const slugPath = pathname === "/docs.md" ? "" : pathname.slice("/docs".length, -".md".length);
    return rewriteToMarkdown(request, slugPath);
  }

  if (pathname === "/docs" || pathname.startsWith("/docs/")) {
    if (isMarkdownPreferred(request)) {
      const slugPath = pathname.slice("/docs".length);
      return rewriteToMarkdown(request, slugPath);
    }
  }

  return NextResponse.next();
}

export const config = {
  matcher: ["/docs", "/docs.md", "/docs/:path*"],
};
