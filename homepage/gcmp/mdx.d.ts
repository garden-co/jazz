declare module "*.mdx" {
  export const meta: {
    slug: string;
    title: string;
    subtitle: string;
    date: string;
    coverImage: string;
    author: {
      name: string;
      image: string;
    };
  };
}
