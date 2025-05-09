"use client";

import clsx from "clsx";
import Link from "next/link";
import { usePathname } from "next/navigation";
import type { ComponentType, ReactNode } from "react";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { isActive } from "../../utils/nav";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { Copyright } from "../atoms/Copyright";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { NewsletterForm } from "./NewsletterForm";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { SocialLinks, type SocialLinksProps } from "./SocialLinks";

type FooterSection = {
  title: string;
  links: {
    href: string;
    label: string;
    newTab?: boolean;
  }[];
};

type FooterProps = {
  logo: ReactNode;
  sections: FooterSection[];
  socials: SocialLinksProps;
  themeToggle: ComponentType<{ className?: string }>;
};

export function Footer({
  logo,
  sections,
  socials,
  themeToggle: ThemeToggle,
}: FooterProps) {
  return (
    <footer className="w-full pt-8 pb-20 mt-12 md:mt-20 md:pb-8">
      <div className="container grid gap-8 md:gap-12">
        <div className="grid grid-cols-12 gap-y-3 sm:items-center pb-8 border-b">
          <div className="col-span-full sm:col-span-6 md:col-span-8">
            <Link href="https://garden.co" target="_blank">
              {logo}
            </Link>
          </div>
          <p className="col-span-full sm:col-span-6 md:col-span-4 text-sm sm:text-base">
            Playful software for serious problems.
          </p>
        </div>
        <div className="grid gap-y-8 grid-cols-12">
          <div className="flex flex-col gap-4 col-span-full md:col-span-8">
            <p className="font-medium text-highlight">Stay up to date</p>
            <NewsletterForm />
          </div>

          {sections.map((section, index) => (
            <div
              key={index}
              className="flex flex-col gap-2 text-sm col-span-6 md:col-span-2"
            >
              <h2 className="font-medium dark:text-muted cursor-default">
                {section.title}
              </h2>
              {section.links.map((link, linkIndex) => (
                <FooterLink
                  key={linkIndex}
                  href={link.href}
                  newTab={link.newTab}
                >
                  {link.label}
                </FooterLink>
              ))}
            </div>
          ))}

          <Copyright className="text-sm order-last col-span-full self-center md:col-span-10 md:order-none" />

          <div className="col-span-full flex items-center justify-between gap-6 md:col-span-2">
            <SocialLinks {...socials}></SocialLinks>
            <ThemeToggle className="hidden md:block" />
          </div>
        </div>
      </div>
    </footer>
  );
}

function FooterLink({
  href,
  className,
  children,
  onClick,
  newTab,
}: {
  href: string;
  className?: string;
  children: ReactNode;
  onClick?: () => void;
  newTab?: boolean;
}) {
  const path = usePathname();

  return (
    <Link
      href={href}
      className={clsx(
        "py-0.5 px-0 text-sm",
        className,
        isActive(href)
          ? "font-medium text-black dark:text-white cursor-default"
          : "text-stone-600 dark:text-stone-400 hover:text-black dark:hover:text-white transition-colors hover:transition-none",
      )}
      onClick={onClick}
      target={newTab ? "_blank" : undefined}
    >
      {children}
      {newTab ? (
        <span className="inline-block text-muted relative -top-0.5 -left-0.5 -mr-2">
          ⌝
        </span>
      ) : (
        ""
      )}
    </Link>
  );
}
