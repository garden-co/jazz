import { FormattedDate } from "@/components/FormattedDate";
import { NewsletterCard } from "@/components/blog/NewsletterCard";
import PostCoverImage from "@/components/blog/PostCoverImage";
import { PostJsonLd } from "@/components/blog/PostJsonLd";
import { BigGrass } from "@/components/blog/Swishes";
import { getPostBySlug, posts } from "@/lib/posts";
import { H1, H2 } from "@garden-co/design-system/src/components/atoms/Headings";
import { Prose } from "@garden-co/design-system/src/components/molecules/Prose";
import { Metadata } from "next";
import Image from "next/image";
import { notFound } from "next/navigation";

export default async function Post({ params }: Params) {
  const post = getPostBySlug(params.slug);

  if (!post) {
    return notFound();
  }

  const { title, subtitle, coverImage, date, author } = post.meta;
  const content = post.default({});

  return (
    <>
      <PostJsonLd
        title={title}
        subtitle={subtitle}
        image={coverImage.replace(".svg", ".png")}
        author={author.name}
        datePublished={date}
      />
      <article className="container max-w-3xl flex flex-col gap-8 py-8 lg:py-16 lg:gap-12">
        <div>
          <H1 className="mb-2">{title}</H1>
          <H2>{subtitle}</H2>
        </div>

        <PostCoverImage src={coverImage} title={title} className="rounded-lg" />
        <div className="flex flex-col gap-2">
          <div className="flex items-center gap-3">
            <Image
              width={100}
              height={100}
              src={author.image}
              className="size-12 rounded-full"
              alt=""
            />
            <div>
              <p className="text-highlight">{author.name}</p>
              <p className="text-sm text-stone-600 ">
                <FormattedDate date={date} />
              </p>
            </div>
          </div>
        </div>

        <Prose size="md" className="text-stone-900 dark:text-stone-50">
          {content}
          <BigGrass />
        </Prose>

        <NewsletterCard />
      </article>
    </>
  );
}

type Params = {
  params: {
    slug: string;
  };
};

export function generateMetadata({ params }: Params): Metadata {
  const post = getPostBySlug(params.slug);

  if (!post) {
    return notFound();
  }

  const { title, subtitle, coverImage } = post.meta;

  return {
    title: title,
    description: subtitle,
    openGraph: {
      title: title,
      images: [coverImage.replace(".svg", ".png")],
    },
    twitter: {
      title: title + " " + subtitle,
      images: [coverImage.replace(".svg", ".png")],
    },
  };
}

export async function generateStaticParams() {
  return posts.map((post) => ({
    slug: post.meta.slug,
  }));
}
