import { SideNav, SideNavBody, SideNavHeader } from "@/components/SideNav";
import { SideNavItem } from "@/components/SideNavItem";
import { packages } from "@/content/packages";
import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import Link from "next/link";
import { requestProject } from "./requestProject";

export function ApiNav({ className }: { className?: string }) {
  return (
    <SideNav className={className}>
      <SideNavBody>
        <SideNavHeader className="mb-5" href="/api-reference">
          API Reference
        </SideNavHeader>
        <ul className="space-y-5">
          {packages.map(({ name }) => (
            <li key={name}>
              <PackageNavItem package={name} />
            </li>
          ))}
        </ul>
      </SideNavBody>
    </SideNav>
  );
}

export async function PackageNavItem({
  package: packageName,
}: {
  package: string;
}) {
  let project = await requestProject(packageName as any);

  return (
    <>
      <SideNavItem
        className="mb-1 flex gap-2 items-center"
        href={`/api-reference/${packageName}`}
      >
        <Icon name="package" size="xs" />
        {packageName}
      </SideNavItem>
      {project.categories?.map((category) => {
        return (
          <details
            key={category.title}
            open={category.title !== "Other"}
            className="group ml-1.5 border-l"
          >
            <summary className="pl-[13px] py-1 cursor-pointer flex gap-2 items-center justify-between hover:text-stone-800 dark:hover:text-stone-200 [&::-webkit-details-marker]:hidden">
              {category.title}

              <Icon
                name="chevronRight"
                size="sm"
                className="text-stone-300 group-open:rotate-90 transition-transform dark:text-stone-800"
              />
            </summary>
            <div className="pl-6">
              {category.children.map(
                (child, i, children) =>
                  (i == 0 || child.name !== children[i - 1]!.name) && (
                    <Link
                      key={child.id}
                      className="block py-0.5 text-ellipsis overflow-hidden font-mono hover:text-stone-800 dark:hover:text-stone-200"
                      href={`/api-reference/${packageName}#${child.name}`}
                    >
                      {child.name}
                    </Link>
                  ),
              )}
            </div>
          </details>
        );
      })}
    </>
  );
}
