"use client";

import {
  CloseButton,
  Popover,
  PopoverButton,
  PopoverGroup,
  PopoverPanel,
} from "@headlessui/react";
import clsx from "clsx";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { type ComponentType, type ReactNode, useEffect, useState } from "react";
import React from "react";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { isActive } from "../../utils/nav";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { Icon } from "../atoms/Icon";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import type { IconName } from "../atoms/Icon";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { BreadCrumb } from "../molecules/Breadcrumb";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { SocialLinks, type SocialLinksProps } from "./SocialLinks";

export type NavItemProps = {
  href: string;
  icon?: IconName;
  title: string;
  firstOnRight?: boolean;
  newTab?: boolean;
  items?: NavItemProps[];
  description?: string;
};

type NavProps = {
  mainLogo: ReactNode;
  items: NavItemProps[];
  cta?: ReactNode;
  socials?: SocialLinksProps;
  themeToggle: ComponentType<{ className?: string }>;
  sections?: NavSection[];
  hideMobileNav?: boolean;
};

export type NavSection = {
  name: string;
  content: ReactNode;
  icon: IconName;
};

function NavItem({
  item,
  className,
}: {
  item: NavItemProps;
  className?: string;
}) {
  const { href, icon, title, items, firstOnRight } = item;
  const active = isActive(href);

  const path = usePathname();

  if (!items?.length) {
    if (item.icon) {
      return (
        <NavLinkLogo className="px-3" {...item}>
          <Icon name={item.icon} />
          <span className="sr-only">{title}</span>
        </NavLinkLogo>
      );
    }

    return (
      <NavLink
        className={clsx(
          className,
          "text-sm px-2 lg:px-4 py-3",
          firstOnRight && "ml-auto",
          active ? "text-highlight" : "",
        )}
        {...item}
      >
        {title}
      </NavLink>
    );
  }

  return (
    <Popover className={clsx("relative", className, firstOnRight && "ml-auto")}>
      <PopoverButton
        className={clsx(
          "flex items-center gap-1.5 text-sm px-2 lg:px-4 py-3 max-sm:w-full hover:text-stone-900 dark:hover:text-white transition-colors hover:transition-none focus-visible:outline-none",
          active ? "text-highlight" : "",
        )}
      >
        <span>{title}</span>
        <Icon name="chevronDown" size="xs" />
      </PopoverButton>

      <PopoverPanel
        transition
        className="absolute left-1/2 -translate-x-1/2 z-10 flex w-screen max-w-[24rem] mt-5 transition data-[closed]:translate-y-1 data-[closed]:opacity-0 data-[enter]:duration-200 data-[leave]:duration-150 data-[enter]:ease-out data-[leave]:ease-in"
      >
        <div className="flex-auto overflow-hidden rounded-lg ring-1 ring-stone-300/60 bg-white/90 backdrop-blur-lg shadow-lg dark:ring-stone-800/50 dark:bg-stone-925/90">
          <div className="p-3 grid">
            {items.map(({ href, title, description, icon }) => (
              <CloseButton
                className="p-3 rounded-md flex gap-3 hover:bg-stone-100/80 dark:hover:bg-stone-900/80 transition-colors"
                href={href}
                aria-label={title}
                as={Link}
                key={href}
              >
                {icon && (
                  <Icon
                    className="stroke-blue dark:stroke-blue-500 shrink-0"
                    size="sm"
                    name={icon}
                  />
                )}
                <div className="grid gap-1.5 mt-px">
                  <p className="text-sm font-medium text-highlight">{title}</p>
                  <p className="text-sm leading-relaxed">{description}</p>
                </div>
              </CloseButton>
            ))}
          </div>
        </div>
      </PopoverPanel>
    </Popover>
  );
}

export function MobileNav({
  mainLogo,
  items,
  socials,
  sections,
  themeToggle: ThemeToggle,
  navBarClassName,
}: NavProps & {
  navBarClassName?: string;
}) {
  const primarySection: {
    name: string;
    icon: IconName;
    content: ReactNode;
  } = {
    name: "Menu",
    icon: "menu",
    content: (
      <>
        <div className="flex items-center justify-between border-b p-3">
          <Link href="/" className="flex items-center">
            {mainLogo}
          </Link>

          <SocialLinks {...socials} className="ml-auto" />

          <ThemeToggle className="ml-4 mr-1" />
        </div>
        <div className="flex flex-col gap-2 p-3">
          {items
            .filter((item) => !("icon" in item))
            .map((item, i) => (
              <NavLink
                className="p-1"
                key={i}
                href={item.href}
                onClick={() => setActive(null)}
                newTab={item.newTab}
              >
                {item.title}
              </NavLink>
            ))}
        </div>
      </>
    ),
  };

  const [active, setActive] = useState<string | null>();

  const pathname = usePathname();

  useEffect(() => {
    setActive(null);
  }, [pathname]);

  const toggle = (type: string) => {
    setActive(active == type ? null : type);
  };

  const navSections = [primarySection, ...(sections || [])];

  return (
    <>
      <div
        className={clsx(
          "md:hidden px-4 flex items-center self-stretch dark:text-white",
          navBarClassName,
        )}
      >
        <NavLinkLogo prominent href="/" className="mr-auto">
          {mainLogo}
        </NavLinkLogo>
        <button
          className="flex gap-2 p-3 rounded-xl items-center"
          onClick={() => {
            setActive("Menu");
          }}
          aria-label="Open menu"
        >
          <Icon name="menu" />
          <BreadCrumb items={items} />
        </button>
      </div>

      <div
        onClick={() => {
          setActive(null);
        }}
        className={clsx(
          !!active ? "block" : "hidden",
          "md:hidden fixed backdrop-blur-sm top-0 bottom-0 left-0 right-0 bg-stone-200/80 dark:bg-black/80 w-full h-full z-20",
        )}
      ></div>

      <div
        className={clsx(
          "md:hidden bg-white border fixed z-50",
          "dark:bg-stone-925",
          active
            ? "rounded-lg right-6 left-6 bottom-6 sm:max-w-lg sm:w-full shadow-md sm:left-1/2 sm:-translate-x-1/2 "
            : "rounded-full shadow-sm left-1/2 -translate-x-1/2  bottom-7",
        )}
      >
        {active && (
          <div
            className={clsx(
              "max-h-[calc(100vh-16rem)] overflow-y-auto",
              active === "Menu" ? "" : "p-3 pb-10",
            )}
          >
            {navSections.map((section) =>
              section.name == active ? (
                <React.Fragment key={section.name}>
                  {section.content}
                </React.Fragment>
              ) : null,
            )}
          </div>
        )}

        <div
          className={clsx("flex justify-center py-1 px-1.5", {
            "border-t py-2": !!active,
          })}
        >
          {navSections.map(
            (section) =>
              section.content && (
                <button
                  type="button"
                  className={clsx(
                    "flex items-center gap-1 px-2 py-1 text-sm rounded-md whitespace-nowrap",
                    "text-highlight",
                    {
                      "bg-stone-100 dark:bg-stone-900": active === section.name,
                    },
                  )}
                  onClick={() => toggle(section.name)}
                  key={section.name}
                >
                  {section.icon && <Icon name={section.icon} size="xs" />}
                  {section.name}
                </button>
              ),
          )}
        </div>
      </div>
    </>
  );
}

function NavLink({
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
        "hover:text-stone-900 dark:hover:text-white transition-colors hover:transition-none",
        className,
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

function NavLinkLogo({
  href,
  className,
  children,
  onClick,
  newTab,
}: {
  href: string;
  className?: string;
  children: ReactNode;
  prominent?: boolean;
  onClick?: () => void;
  newTab?: boolean;
}) {
  return (
    <Link
      href={href}
      className={clsx(
        "py-3 hover:text-stone-900 dark:hover:text-white",
        className,
      )}
      onClick={onClick}
      target={newTab ? "_blank" : undefined}
    >
      {children}
    </Link>
  );
}

export function Nav(props: NavProps) {
  const { mainLogo, items, cta, hideMobileNav } = props;
  return (
    <>
      <div className="w-full border-b py-2 sticky top-0 z-50 bg-white dark:bg-stone-950 hidden md:block">
        <PopoverGroup className="flex flex-wrap items-center max-sm:justify-between md:gap-2 container w-full">
          <Link href="/" className="flex items-center">
            {mainLogo}
          </Link>

          {items.map((item, i) => (
            <NavItem
              key={i}
              item={item}
              className={i == items.length - 1 ? "mr-3" : ""}
            />
          ))}

          <SocialLinks
            {...props.socials}
            className={
              !items.find((item) => item.firstOnRight) ? "ml-auto" : ""
            }
          />

          {cta}
        </PopoverGroup>
      </div>
      {!hideMobileNav && <MobileNav {...props} />}
    </>
  );
}
