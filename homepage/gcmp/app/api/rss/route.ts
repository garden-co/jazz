import { metaTags } from "@/app/layout";
import { posts } from "@/lib/posts";
import { Feed } from "feed";
import { NextResponse } from "next/server";

export async function GET() {
  const feed = new Feed({
    title: "Garden Computing Blog",
    description: "News from Garden Computing",
    id: metaTags.url,
    link: `${metaTags.url}/news`,
    language: "en",
    image: `${metaTags.url}/social-image.png`,
    favicon: `${metaTags.url}/favicon.ico`,
    copyright: `${new Date().getFullYear()} Garden Computing, Inc.`,
  });

  posts.forEach((post) => {
    feed.addItem({
      title: post.meta.title,
      description: post.meta.subtitle,
      id: post.meta.slug,
      link: `${metaTags.url}/news/${post.meta.slug}`,
      date: new Date(post.meta.date),
      author: [{ name: post.meta.author.name }],
      guid: post.meta.slug,
      image: `${metaTags.url}${post.meta.coverImage}`,
    });
  });

  return new NextResponse(feed.rss2(), {
    headers: {
      "Content-Type": "application/xml",
      "Cache-Control": "public, s-maxage=3600, stale-while-revalidate=86400",
    },
  });
}
