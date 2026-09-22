import { Feed } from "feed";
import { blogSource } from "@/lib/source";

const baseUrl =
  process.env.NEXT_PUBLIC_SITE_URL ??
  (process.env.VERCEL_PROJECT_PRODUCTION_URL
    ? `https://${process.env.VERCEL_PROJECT_PRODUCTION_URL}`
    : "http://localhost:3000");

export function getRSS() {
  const feed = new Feed({
    title: "Jazz Blog",
    description: "Long-form writing about Jazz, local-first systems, sync, and the cloud.",
    id: `${baseUrl}/blog`,
    link: `${baseUrl}/blog`,
    language: "en",
  });

  const posts = [...blogSource.getPages()].sort(
    (a, b) => new Date(a.data.date).getTime() - new Date(b.data.date).getTime(),
  );

  for (const page of posts) {
    feed.addItem({
      id: `${baseUrl}${page.url}`,
      title: page.data.title,
      description: page.data.description,
      link: `${baseUrl}${page.url}`,
      date: new Date(page.data.date),
      author: [{ name: page.data.author }],
    });
  }

  return feed.rss2();
}
