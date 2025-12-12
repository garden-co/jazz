import type { StaticImageData } from "next/image";
import * as HelloWorldPost from "@/components/blog/posts/1_helloWorld.mdx";
// import * as WhatIsJazzPost from "@/components/blog/posts/2_whatIsJazz.mdx";
// import * as WhatWeShippedSinceSummerPost from "@/components/blog/posts/3_whatWeShippedSinceSummer.mdx";

type PostMDX = {
  meta: {
    slug: string;
    title: string;
    subtitle: string;
    date: string;
    coverImage: string | StaticImageData;
    coverImagePng?: string | StaticImageData;
    author: { name: string; image: string | StaticImageData };
  };
  default: (props: {
    components?: Record<string, React.ComponentType>;
  }) => React.ReactNode;
};

export const posts: PostMDX[] = [HelloWorldPost as PostMDX];

export const getPostBySlug = (slug: string) => {
  return posts.find((post) => post.meta.slug === slug);
};
