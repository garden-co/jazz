import Link from "next/link";
import type { Metadata } from "next";
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
      <section className="w-full pb-24 pt-18 sm:pb-28 sm:pt-22 lg:pb-32 lg:pt-26">
        <div className="mx-auto w-full max-w-(--fd-layout-width) px-4">
          <div className="max-w-[42rem] space-y-4">
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
              Blog
            </p>
            <h1 className="text-[clamp(3rem,8vw,5.5rem)] font-black leading-[0.9] tracking-[-0.05em]">
              Longer notes on what we&apos;re building
            </h1>
            <p className="max-w-[38rem] text-lg leading-relaxed text-fd-muted-foreground sm:text-xl">
              Essays, technical deep dives, and launch writing about Jazz, sync, local-first data,
              and the infrastructure around it.
            </p>
          </div>
          <div className="mt-18 grid gap-x-12 gap-y-12 md:grid-cols-2 lg:gap-x-16">
            {posts.map((post) => (
              <Link key={post.url} href={post.url} className="group block border-t pt-4">
                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
                  {dateFormatter.format(new Date(post.data.date))}
                </p>
                <h2 className="mt-3 text-3xl font-black leading-[0.92] tracking-[-0.04em] transition-colors group-hover:text-fd-primary">
                  {post.data.title}
                </h2>
                <p className="mt-4 max-w-[34rem] text-base leading-relaxed text-fd-muted-foreground">
                  {post.data.description}
                </p>
                <p className="mt-4 text-sm font-medium">By {post.data.author}</p>
              </Link>
            ))}
          </div>
        </div>
      </section>
    </div>
  );
}
