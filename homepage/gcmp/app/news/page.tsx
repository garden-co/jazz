import { HeroHeader } from "@garden-co/design-system/src/components/molecules/HeroHeader";
import { NewsletterCard } from "@/components/blog/NewsletterCard";
import { Posts } from "@/components/blog/Posts";

export const metadata = {
  title: "Blog",
};

export default function NewsPage() {
  return (
    <div className="container flex flex-col gap-10">
      <HeroHeader title="Blog" slogan="" />

      <Posts />

      <NewsletterCard />
    </div>
  );
}
