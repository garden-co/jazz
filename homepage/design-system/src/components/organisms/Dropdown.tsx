"use client";

import * as DropdownMenuPrimitive from "@radix-ui/react-dropdown-menu";
import clsx from "clsx";
import Link from "next/link";
import * as React from "react";
import {
  Style,
  styleToTextDarkMap,
  styleToTextMap,
} from "../../utils/tailwindClassesMap";
import { Button } from "../atoms/Button";

export const Dropdown = DropdownMenuPrimitive.Root;

export const DropdownTrigger = DropdownMenuPrimitive.Trigger;

export function DropdownButton<T extends React.ElementType = typeof Button>({
  as,
  ...props
}: { as?: T } & React.ComponentProps<T>) {
  const Component = as ?? Button;
  return (
    <DropdownMenuPrimitive.Trigger asChild>
      <Component
        icon={props.icon || "chevronDown"}
        iconPosition="right"
        variant={props.variant || "outline"}
        {...props}
      />
    </DropdownMenuPrimitive.Trigger>
  );
}

export const DropdownMenu = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.Content>
>(({ className, sideOffset = 8, ...props }, ref) => (
  <DropdownMenuPrimitive.Portal>
    <DropdownMenuPrimitive.Content
      ref={ref}
      sideOffset={sideOffset}
      className={clsx(
        className,
        "isolate z-50 min-w-[12rem] overflow-hidden rounded-lg p-1.5",
        "bg-white/75 backdrop-blur-xl dark:bg-stone-925",
        "shadow-lg ring-1 ring-stone-950/10 dark:ring-inset dark:ring-white/10",
        "supports-[grid-template-columns:subgrid]:grid supports-[grid-template-columns:subgrid]:grid-cols-[auto_1fr_1.5rem_0.5rem_auto]",
        "data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95 data-[side=bottom]:slide-in-from-top-2 data-[side=left]:slide-in-from-right-2 data-[side=right]:slide-in-from-left-2 data-[side=top]:slide-in-from-bottom-2",
      )}
      {...props}
    />
  </DropdownMenuPrimitive.Portal>
));
DropdownMenu.displayName = DropdownMenuPrimitive.Content.displayName;

export const DropdownItem = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.Item>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.Item> & {
    intent?: Style | undefined;
    href?: string;
    selected?: boolean;
    selectedItemColor?: Style;
  }
>(
  (
    {
      className,
      intent,
      href,
      selected = false,
      selectedItemColor = "primary",
      ...props
    },
    ref,
  ) => {
    const effectiveIntent =
      selected && !intent ? selectedItemColor : intent || "default";

    const getTextColor = () => {
      if (selected && !!intent) {
        return styleToTextDarkMap[
          effectiveIntent as keyof typeof styleToTextDarkMap
        ];
      }
      return styleToTextMap[effectiveIntent as keyof typeof styleToTextMap];
    };

    const classes = clsx(
      className,
      "group rounded-md space-x-2 focus:outline-none px-2.5 py-1.5 cursor-pointer select-none transition-colors",
      "text-left text-sm/6 forced-colors:text-[CanvasText]",
      "data-[highlighted]:bg-stone-100 dark:data-[highlighted]:bg-stone-900",
      "data-[disabled]:opacity-50",
      "forced-color-adjust-none forced-colors:data-[highlighted]:bg-[Highlight] forced-colors:data-[highlighted]:text-[HighlightText] forced-colors:[&>[data-slot=icon]]:data-[highlighted]:text-[HighlightText]",
      "col-span-full grid grid-cols-[auto_1fr_1.5rem_0.5rem_auto] items-center",
      "[&>[data-slot=icon]]:col-start-1 [&>[data-slot=icon]]:row-start-1 [&>[data-slot=icon]]:-ml-0.5 [&>[data-slot=icon]]:mr-2.5 [&>[data-slot=icon]]:size-5 sm:[&>[data-slot=icon]]:mr-2 [&>[data-slot=icon]]:sm:size-4",
      "[&>[data-slot=icon]]:text-stone-500 [&>[data-slot=icon]]:data-[highlighted]:text-stone-700 [&>[data-slot=icon]]:dark:data-[highlighted]:text-white",
      "[&>[data-slot=avatar]]:mr-2.5 [&>[data-slot=avatar]]:size-6 sm:[&>[data-slot=avatar]]:mr-2 sm:[&>[data-slot=avatar]]:size-5",
      getTextColor(),
    );

    return (
      <DropdownMenuPrimitive.Item
        ref={ref}
        className={classes}
        {...props}
        asChild
      >
        {href ? (
          <Link href={href}>{props.children}</Link>
        ) : (
          <div>{props.children}</div>
        )}
      </DropdownMenuPrimitive.Item>
    );
  },
);
DropdownItem.displayName = DropdownMenuPrimitive.Item.displayName;

export function DropdownHeader({
  className,
  ...props
}: React.ComponentPropsWithoutRef<"div">) {
  return (
    <div
      {...props}
      className={clsx(className, "col-span-5 px-3.5 pb-1 pt-2.5 sm:px-3")}
    />
  );
}

export const DropdownSection = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.Group>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.Group>
>(({ className, ...props }, ref) => (
  <DropdownMenuPrimitive.Group
    ref={ref}
    className={clsx(
      className,
      "col-span-full supports-[grid-template-columns:subgrid]:grid supports-[grid-template-columns:subgrid]:grid-cols-[auto_1fr_1.5rem_0.5rem_auto]",
    )}
    {...props}
  />
));
DropdownSection.displayName = DropdownMenuPrimitive.Group.displayName;

export const DropdownHeading = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.Label>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.Label>
>(({ className, ...props }, ref) => (
  <DropdownMenuPrimitive.Label
    ref={ref}
    className={clsx(
      className,
      "col-span-full grid grid-cols-[1fr,auto] gap-x-12 px-3.5 pb-1 pt-2 text-sm/5 font-medium text-stone-500 sm:px-3 sm:text-xs/5",
    )}
    {...props}
  />
));
DropdownHeading.displayName = DropdownMenuPrimitive.Label.displayName;

export const DropdownDivider = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.Separator>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.Separator>
>(({ className, ...props }, ref) => (
  <DropdownMenuPrimitive.Separator
    ref={ref}
    className={clsx(
      className,
      "col-span-full mx-3.5 my-1 h-px border-0 bg-stone-950/5 sm:mx-3 dark:bg-white/10 forced-colors:bg-[CanvasText]",
    )}
    {...props}
  />
));
DropdownDivider.displayName = DropdownMenuPrimitive.Separator.displayName;

export function DropdownLabel({
  className,
  ...props
}: React.ComponentPropsWithoutRef<"span">) {
  return (
    <span
      {...props}
      data-slot="label"
      className={clsx(className, "text-highlight col-start-2 row-start-1")}
    />
  );
}

export const DropdownDescription = React.forwardRef<
  React.ElementRef<"p">,
  React.ComponentPropsWithoutRef<"p">
>(({ className, ...props }, ref) => (
  <p
    ref={ref}
    data-slot="description"
    className={clsx(
      className,
      "col-span-2 col-start-2 row-start-2 text-sm/5 text-stone-500 group-data-[highlighted]:text-white sm:text-xs/5  forced-colors:group-data-[highlighted]:text-[HighlightText]",
    )}
    {...props}
  />
));
DropdownDescription.displayName = "DropdownDescription";

export function DropdownShortcut({
  keys,
  className,
  ...props
}: {
  keys: string | string[];
  className?: string;
} & React.ComponentPropsWithoutRef<"div">) {
  return (
    <div
      {...props}
      className={clsx(
        className,
        "col-start-5 row-start-1 flex justify-self-end",
      )}
    >
      {(Array.isArray(keys) ? keys : keys.split("")).map((char, index) => (
        <kbd
          key={index}
          className={clsx([
            "min-w-[2ch] text-center font-sans capitalize text-stone-400 group-data-[highlighted]:text-white forced-colors:group-data-[highlighted]:text-[HighlightText]",
            index > 0 && char.length > 1 && "pl-1",
          ])}
        >
          {char}
        </kbd>
      ))}
    </div>
  );
}
