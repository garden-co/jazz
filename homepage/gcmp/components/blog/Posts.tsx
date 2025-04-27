import { FormattedDate } from "@/components/FormattedDate";
import PostCoverImage from "@/components/blog/PostCoverImage";
import { posts } from "@/lib/posts";
import Link from "next/link";

export function Posts() {
  return (
    <div className="grid md:grid-cols-3 gap-8">
      {posts.map((post) => (
        <div className="flex flex-col gap-2" key={post.meta.slug}>
          <PostCoverImage
            src={post.meta.coverImage}
            title={post.meta.title}
            slug={post.meta.slug}
            className="mb-1.5 rounded-lg"
          />
          <Link href={`/news/${post.meta.slug}`} className="text-highlight">
            <h1 className="text-2xl font-display font-semibold tracking-tight">
              {post.meta.title}
            </h1>
            <h2 className="text-lg">{post.meta.subtitle}</h2>
          </Link>
          <div className="flex text-sm items-center">
            {post.meta.author.name} • <FormattedDate date={post.meta.date} />
          </div>
        </div>
      ))}
    </div>
  );
}
