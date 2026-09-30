import type { Metadata } from "next";
import { Heading } from "@astryxdesign/core/Heading";
import { Link } from "@astryxdesign/core/Link";
import { Text } from "@astryxdesign/core/Text";
import { blogSource } from "@/lib/source";

export const metadata: Metadata = {
  title: "Blog",
  description: "Long-form writing about Jazz, local-first systems, sync, and the cloud.",
};

const dateFormatter = new Intl.DateTimeFormat("en-US", {
  dateStyle: "long",
});

export default function BlogIndexPage() {
  const posts = [...blogSource.getPages()].sort(
    (left, right) => new Date(right.data.date).getTime() - new Date(left.data.date).getTime(),
  );

  return (
    <div className="w-full">
      <section className="home-container pb-12 pt-16 sm:pt-20">
        <div className="grid gap-6 lg:grid-cols-12 lg:items-end">
          <Heading level={1} type="display-2" className="lg:col-span-7">
            Blog
          </Heading>
          <div className="lg:col-span-5">
            <Text as="p" display="block" type="large" color="secondary" weight="normal">
              Essays, technical deep dives and launch writing about Jazz, sync, local-first data and
              the infrastructure around it.
            </Text>
            <Text as="p" display="block" color="secondary" className="mt-3">
              Follow along with the <Link href="/rss.xml">RSS feed</Link>.
            </Text>
          </div>
        </div>
      </section>
      <section className="home-container pb-24">
        <ol className="blog-list">
          {posts.map((post) => (
            <li key={post.url} className="blog-list-item">
              <Text
                as="p"
                display="block"
                type="supporting"
                color="secondary"
                className="blog-list-meta"
              >
                <time dateTime={new Date(post.data.date).toISOString().slice(0, 10)}>
                  {dateFormatter.format(new Date(post.data.date))}
                </time>
                <span className="block">{post.data.author}</span>
              </Text>
              <div className="blog-list-body">
                <Heading level={2}>
                  <a href={post.url} className="blog-list-link">
                    {post.data.title}
                  </a>
                </Heading>
                {post.data.description ? (
                  <Text as="p" display="block" color="secondary" className="mt-2">
                    {post.data.description}
                  </Text>
                ) : null}
              </div>
            </li>
          ))}
        </ol>
      </section>
    </div>
  );
}
