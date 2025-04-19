"use client";

import Link from "next/link";
import { AnchorHTMLAttributes, DetailedHTMLProps } from "react";

import { Button } from "@garden-co/design-system/src/components/atoms/Button";
import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";

export function FileDownloadLink(
  props: DetailedHTMLProps<
    AnchorHTMLAttributes<HTMLAnchorElement>,
    HTMLAnchorElement
  >,
) {
  if (!props.href) {
    return props.children;
  }

  const { children, href } = props;

  return (
    <div className="inline-flex items-center font-medium text-stone-900 rounded-md border p-3 shadow-sm dark:text-white dark:bg-stone-925 flex py-2 rounded-lg ">
      <Icon name="file" size="sm" className="mr-2" />
      {children}

      <a href={href} download className="ml-12">
        Download
      </a>
    </div>
  );
}
