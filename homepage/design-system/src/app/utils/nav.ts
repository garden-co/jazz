import { usePathname } from "next/navigation";

export function isActive(href: string) {
  const path = usePathname();

  if (href === "/") {
    return path === "/";
  }

  return path.startsWith(href);
}
