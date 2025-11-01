import clsx from "clsx";
import type { ReactNode } from "react";
import { H1, H2 } from "../atoms/Headings";

function H1Sub({ children }: { children: React.ReactNode }) {
  return (
    <p className="text-lg text-pretty leading-relaxed max-w-3xl dark:text-stone-200 md:text-xl">
      {children}
    </p>
  );
}

type HeroHeaderProps = {
  title: ReactNode;
  slogan?: ReactNode;
  pt?: boolean;
  className?: string;
  level?: "h1" | "h2";
};

export function HeroHeader({
  title,
  slogan,
  pt = true,
  className = "",
  level = "h1",
}: HeroHeaderProps) {
  const Heading = level === "h2" ? H2 : H1;

  return (
    <hgroup
      className={clsx(pt && "pt-12 md:pt-20", "mb-10 grid gap-2", className)}
    >
      <Heading>{title}</Heading>
      {slogan && <H1Sub>{slogan}</H1Sub>}
    </hgroup>
  );
}
