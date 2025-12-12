import type { StaticImageData } from "next/image";

declare module "*.mdx" {
  export const meta: {
    slug: string;
    title: string;
    subtitle: string;
    date: string;
    coverImage: string | StaticImageData;
    coverImagePng?: string | StaticImageData;
    author: {
      name: string;
      image: string | StaticImageData;
    };
  };
}
