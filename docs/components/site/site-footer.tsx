import { Link } from "@astryxdesign/core/Link";
import { Text } from "@astryxdesign/core/Text";
import { GardenComputingLogo } from "@/components/brand/garden-computing-logo";
import { JazzLogo } from "@/components/brand/jazz-logo";
import { gitConfig } from "@/lib/layout.shared";

const columns = [
  {
    title: "Product",
    links: [
      { label: "Docs", href: "/docs" },
      { label: "Quickstart", href: "/docs/setup/run-locally" },
      { label: "Examples & benches", href: "/examples" },
      { label: "Pricing", href: "/#pricing" },
      { label: "Roadmap", href: "https://github.com/garden-co/jazz/milestones" },
    ],
  },
  {
    title: "Resources",
    links: [
      { label: "Blog", href: "/blog" },
      { label: "RSS feed", href: "/rss.xml" },
      { label: "llms.txt", href: "/llms.txt" },
      { label: "Classic Jazz", href: "https://classic.jazz.tools" },
    ],
  },
  {
    title: "Community",
    links: [
      { label: "GitHub", href: `https://github.com/${gitConfig.user}/${gitConfig.repo}` },
      { label: "Discord", href: "https://discord.gg/RN9UKh52be" },
      { label: "X", href: "https://x.com/jazz_tools" },
    ],
  },
];

/** The footer on pages without a sidebar (homepage, blog). */
export function SiteFooter() {
  return (
    <footer className="site-footer w-full">
      <div className="mx-auto grid w-full max-w-(--fd-layout-width) grid-cols-2 gap-x-6 gap-y-12 px-4 py-14 md:grid-cols-12">
        <div className="col-span-2 flex flex-col gap-4 md:col-span-6">
          <JazzLogo label="Jazz" className="h-7 self-start" />
          <Text as="p" display="block" color="secondary" className="max-w-[22rem]">
            The data layer that you own.
          </Text>
        </div>
        {columns.map((column) => (
          <nav key={column.title} aria-label={column.title} className="md:col-span-2">
            <Text as="h2" display="block" type="label" color="secondary">
              {column.title}
            </Text>
            <ul className="mt-4 flex flex-col gap-2.5">
              {column.links.map((link) => (
                <li key={link.href}>
                  <Link href={link.href} color="inherit" className="site-footer-link">
                    {link.label}
                  </Link>
                </li>
              ))}
            </ul>
          </nav>
        ))}
      </div>
      <div className="site-footer-base">
        <div className="mx-auto grid w-full max-w-(--fd-layout-width) grid-cols-1 items-center gap-2 px-4 py-6 sm:grid-cols-3 sm:gap-4">
          <a href="https://garden.co" className="site-footer-maker" aria-label="Garden Computing">
            <GardenComputingLogo aria-hidden className="h-10" label="" role="presentation" />
          </a>
          <Text as="p" display="block" color="secondary" className="sm:text-center">
            Made with love and LLMs
          </Text>
          <Text
            as="p"
            display="block"
            type="supporting"
            color="secondary"
            className="mt-4 sm:mt-0 sm:text-right"
          >
            © {new Date().getFullYear()} Garden Computing
          </Text>
        </div>
      </div>
    </footer>
  );
}
