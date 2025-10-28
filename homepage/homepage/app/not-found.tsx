"use client";
import { Button } from "@garden-co/design-system/src/components/atoms/Button";
import { SiDiscord } from "@icons-pack/react-simple-icons";
import { JazzMobileNav } from "@/components/nav";
import { usePathname } from "next/navigation";
import { JazzNav } from "@/components/nav";
import { SearchBoxWithResults } from '@/components/SearchBoxWithResults';
import Link from "next/link";

export default function NotFound() {
  const path = usePathname();

  return (
    <>
      <div className="w-full">
        <JazzNav />
        <div className="pb-8 pt-[calc(61px+2rem)] md:pt-8 md:max-w-3xl mx-auto">
          <hgroup className="pt-12 md:pt-20 mb-10">
            <h1 className="text-stone-950 dark:text-white font-display text-5xl lg:text-6xl mb-3 font-medium tracking-tighter">
              Page Not Found
            </h1>
            <p className="text-lg text-pretty leading-relaxed max-w-3xl dark:text-stone-200 md:text-xl">
              Either the link you followed is broken or the content has moved.</p>
          </hgroup>
          <label htmlFor="search-in-searchbox" className="font-medium">Were you looking for...</label>
          <SearchBoxWithResults searchTerms={path.replace('/docs', '').split('/').join(' ').trim()} />

          <p className="my-4">Still couldn't find what you were looking for? <a href="https://discord.gg/utDMjHYg42" className="underline inline-flex items-center gap-2">Let us know on Discord! <SiDiscord /></a></p>


          <p>
            <a
              href="https://discord.gg/utDMjHYg42"
              className="underline"
            >
              Go home &rarr;
            </a>
          </p>
          <JazzMobileNav />
        </div>
      </div>
    </>
  );
}
