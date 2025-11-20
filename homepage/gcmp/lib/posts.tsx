import * as HelloWorldPost from "@/components/blog/posts/1_helloWorld.mdx";
import * as WhatIsJazzPost from "@/components/blog/posts/2_whatIsJazz.mdx";

export const posts: (typeof HelloWorldPost)[] = [
  HelloWorldPost,
  WhatIsJazzPost,
];

export const getPostBySlug = (slug: string) => {
  return posts.find((post) => post.meta.slug === slug);
};
