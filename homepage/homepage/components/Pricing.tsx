import { Button } from "@garden-co/design-system/src/components/atoms/Button";
import { clsx } from "clsx";
import {
  CircleCheckIcon,
  LucideBuilding2,
  LucideChevronsUp,
  LucideCloudDownload,
  LucideDatabase,
  LucideHandshake,
  LucideIcon,
  LucideServer,
  LucideUsers,
  LucideCloud,
  LucideDatabaseZap,
  LucideImagePlay,
  LucideFileDown,
  LucideAppWindowMac,
} from "lucide-react";
import {
  IndieTierLogo,
  ProTierLogo,
  StarterTierLogo,
  EnterpriseTierLogo,
} from "./TierLogos";
import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import { H2 } from "@garden-co/design-system/src/components/atoms/Headings";

export function ListItem({
  variant = "blue",
  icon: Icon = CircleCheckIcon,
  className = "",
  children,
}: {
  variant?: "gray" | "blue" | "enterprise";
  icon: LucideIcon;
  className?: string;
  children: React.ReactNode;
}) {
  const iconSize = 16;

  const iconVariants = {
    gray: <Icon size={iconSize} className="mt-1 shrink-0 text-stone-500" />,
    blue: (
      <Icon
        size={iconSize}
        className="mt-1 shrink-0 text-primary dark:text-white"
      />
    ),
    enterprise: (
      <Icon
        size={iconSize}
        className="mt-1 shrink-0 text-black dark:text-white"
      />
    ),
  };

  return (
    <li
      className={clsx(
        "inline-flex gap-3 py-2 text-stone-800 dark:text-stone-200",
        className,
      )}
    >
      {iconVariants[variant]}
      <span>{children}</span>
    </li>
  );
}

export function Pricing() {
  return (
    <>
      {/* Self-hosted option banner */}
      <div className="bg-linear-to-r shadow-xs rounded-xl border from-stone-50 to-gray-50 p-6 shadow-gray-900/5 dark:from-stone-925 dark:to-stone-950">
        <div className="flex items-start gap-4">
          <div className="grid w-full items-center gap-4 md:flex">
            <div className="grid grow gap-2">
              <h3 className="flex w-full items-center justify-between text-xl font-semibold text-stone-900 dark:text-white">
                <span className="flex items-center gap-1.5">
                  <Icon name="server" />
                  Self-Hosted (Open-Source)
                </span>
              </h3>

              <p className="text-base text-stone-600 dark:text-stone-400">
                Self-host Jazz for complete control. Can be combined with Jazz Cloud for hybrid deployments.
              </p>
            </div>

            <Button
              href="/docs/react/core-concepts/sync-and-storage#self-hosting-your-sync-server"
              variant="outline"
              intent="primary"
              className="whitespace-nowrap"
            >
              Learn more
              <Icon name="chevronRight" intent="primary" />
            </Button>
          </div>
        </div>
      </div>

      <div className="mb-10 grid gap-4 md:grid-cols-4">
        <div className="shadow-xs flex flex-col items-start gap-3 rounded-xl border bg-stone-100 p-6 shadow-gray-900/5 dark:bg-stone-925">
          <h3 className="flex w-full items-center justify-between text-xl font-semibold text-stone-900 dark:text-white">
            <span className="flex items-center gap-1.5">
              <StarterTierLogo />
              Starter
            </span>
            <span className="ml-auto text-highlight">
              <span className="text-2xl font-light tabular-nums tracking-tighter">
                $0
              </span>
              <span className="text-sm font-normal text-stone-600 dark:text-stone-500">
                /mo
              </span>
            </span>
          </h3>

          <p className="text-sm">Everything you need to get started.</p>

          <ul className="my-4 mb-auto flex w-full flex-col text-sm lg:text-base">
            <ListItem icon={LucideAppWindowMac}>Unlimited apps</ListItem>
            <ListItem icon={LucideDatabase}>Unlimited CoValues</ListItem>
            <li aria-hidden="true" className="my-2 list-none border-t-2" />
            <ListItem icon={LucideUsers}>
              <span className="tabular-nums">100</span> monthly active users
            </ListItem>
            <ListItem icon={LucideDatabase}>
              <span className="tabular-nums">10</span> GB storage
            </ListItem>
            <ListItem icon={LucideCloudDownload}>
              <span className="tabular-nums">0.2</span> GB sync egress
            </ListItem>
            <ListItem icon={LucideFileDown}>
              <span className="tabular-nums">2.0</span> GB blob egress
            </ListItem>
            <li aria-hidden="true" className="my-2 list-none border-t-2" />
            <ListItem icon={LucideCloud}>Optimal cloud routing</ListItem>
            <ListItem icon={LucideDatabaseZap}>Smart caching</ListItem>
          </ul>

          <Button
            href="https://dashboard.jazz.tools?utm_source=cloud_cta_starter"
            newTab
            variant="outline"
            intent="primary"
          >
            Get Starter API key
          </Button>

          <p className="text-sm">No credit card required. Takes 20s.</p>
        </div>
        <div className="shadow-xs flex flex-col items-start gap-3 rounded-xl border bg-stone-100 p-6 shadow-gray-900/5 dark:bg-stone-925">
          <h3 className="flex w-full items-center justify-between text-xl font-semibold text-stone-900 dark:text-white">
            <span className="flex items-center gap-1.5">
              <IndieTierLogo />
              Indie
            </span>
            <span className="text-highlight">
              <span className="text-2xl font-light tabular-nums tracking-tighter">
                $4
              </span>
              <span className="text-sm font-normal text-stone-600 dark:text-stone-500">
                /mo
              </span>
            </span>
          </h3>

          <p className="text-sm">Launch your apps to lots of users.</p>

          <ul className="my-4 mb-auto flex w-full flex-col text-sm lg:text-base">
            <ListItem icon={LucideAppWindowMac}>Unlimited apps</ListItem>
            <ListItem icon={LucideDatabase}>Unlimited CoValues</ListItem>
            <li aria-hidden="true" className="my-2 list-none border-t-2" />
            <ListItem icon={LucideUsers}>
              <span className="tabular-nums">10,000</span> monthly active users
            </ListItem>
            <ListItem icon={LucideDatabase}>
              <span className="tabular-nums">100</span> GB storage
            </ListItem>
            <ListItem icon={LucideCloudDownload}>
              <span className="tabular-nums">2</span> GB sync egress
            </ListItem>
            <ListItem icon={LucideFileDown}>
              <span className="tabular-nums">20</span> GB blob egress
            </ListItem>
            <li aria-hidden="true" className="my-2 list-none border-t-2" />
            <ListItem icon={LucideCloud}>Optimal cloud routing</ListItem>
            <ListItem icon={LucideDatabaseZap}>Smart caching</ListItem>
          </ul>

          <Button
            href="https://dashboard.jazz.tools?utm_source=cloud_cta_indie"
            newTab
            intent="primary"
          >
            Get Indie API key
          </Button>
          <p className="text-sm">One month free trial. Takes 1min.</p>
        </div>
        <div className="shadow-xs flex flex-col items-start gap-3 rounded-xl border border-primary bg-stone-100 p-6 shadow-gray-900/5 dark:bg-stone-925">
          <h3 className="flex w-full items-center justify-between text-xl font-semibold text-stone-900 dark:text-white">
            <span className="flex items-center gap-1.5">
              <ProTierLogo />
              Pro
            </span>
            <span className="text-highlight">
              <span className="text-lg font-normal">from</span>{" "}
              <span className="text-2xl font-light tabular-nums tracking-tighter">
                $19
              </span>
              <span className="text-sm font-normal text-stone-600 dark:text-stone-500">
                /mo
              </span>
            </span>
          </h3>

          <p className="text-sm">Scale to millions at predictable costs.</p>

          <ul className="my-4 mb-auto flex w-full flex-col text-sm lg:text-base">
            <ListItem icon={LucideAppWindowMac}>Unlimited apps</ListItem>
            <ListItem icon={LucideDatabase}>Unlimited CoValues</ListItem>
            <li aria-hidden="true" className="my-2 list-none border-t-2" />
            <ListItem icon={LucideUsers}>Unlimited users</ListItem>
            <ListItem icon={LucideDatabase}>
              <span className="tabular-nums">$0.02/GB</span> storage
            </ListItem>
            <ListItem icon={LucideCloudDownload}>
              <span className="tabular-nums">$1.00/GB</span> sync egress
            </ListItem>
            <ListItem icon={LucideFileDown}>
              <span className="tabular-nums">$0.10/GB</span> blob egress
            </ListItem>
            <li aria-hidden="true" className="my-2 list-none border-t-2" />
            <ListItem icon={LucideCloud}>Optimal cloud routing</ListItem>
            <ListItem icon={LucideDatabaseZap}>Smart caching</ListItem>

            <li aria-hidden="true" className="my-2 list-none border-t-2" />
            <ListItem icon={LucideChevronsUp}>High-priority sync</ListItem>
          </ul>

          <Button
            href="https://dashboard.jazz.tools?utm_source=cloud_cta_pro"
            newTab
            intent="primary"
          >
            Get Pro API key
          </Button>

          <p className="text-sm">One month free trial. Takes 1min.</p>
        </div>
        <div className="shadow-xs flex flex-col items-start gap-3 rounded-xl border border-black dark:border-white bg-stone-100 p-6 shadow-gray-900/5 dark:bg-stone-925">
          <h3 className="flex w-full items-center justify-between text-xl font-semibold text-stone-900 dark:text-white">
            <span className="flex items-center gap-1.5">
              <EnterpriseTierLogo />
            </span>
          </h3>

          <p className="text-sm">Jazz Cloud tailored for enterprise.</p>

          <ul className="my-4 flex w-full flex-col text-sm lg:text-base">
            <ListItem variant="enterprise" icon={LucideServer}>Dedicated / on-prem cloud</ListItem>
            <ListItem variant="enterprise" icon={LucideDatabase}>Unlimited CoValues</ListItem>

            <li aria-hidden="true" className="my-2 list-none border-t-2" />
            <ListItem variant="enterprise" icon={LucideUsers}>
              Unlimited users
            </ListItem>
            <ListItem variant="enterprise" icon={LucideDatabase}>
              Bundled storage
            </ListItem>
            <ListItem variant="enterprise" icon={LucideCloudDownload}>
              Bundled sync egress
            </ListItem>
            <ListItem variant="enterprise" icon={LucideFileDown}>
              Bundled blob egress
            </ListItem>

            <li aria-hidden="true" className="my-2 list-none border-t-2" />
            <ListItem variant="enterprise" icon={LucideCloud}>
              Optimal cloud routing
            </ListItem>
            <ListItem variant="enterprise" icon={LucideDatabaseZap}>
              Smart caching
            </ListItem>
            <li aria-hidden="true" className="my-2 list-none border-t-2" />
            <ListItem variant="enterprise" icon={LucideChevronsUp}>
              Guaranteed sync capacity
            </ListItem>
            <li aria-hidden="true" className="my-2 list-none border-t-2" />
            <ListItem variant="enterprise" icon={LucideHandshake}>
              Dedicated integration team
            </ListItem>
            <ListItem variant="enterprise" icon={LucideBuilding2}>
              SLAs & support contracts
            </ListItem>
          </ul>

          <div className="flex gap-2">
            <Button
              href="https://cal.com/anselm-io/cloud-pro-intro"
              intent="strong"
              newTab
            >
              Book a Call
            </Button>
            {/* <Button
              href="/enterprise"
              intent="strong"
              variant="outline"
            >
              Learn More
            </Button> */}
          </div>

          <p className="text-sm">
            Alternatively,{" "}
            <a
              href="mailto:sales@garden.co"
              className="text-black dark:text-white underline"
            >
              contact sales by email
            </a>
            .
          </p>
        </div>
      </div>
    </>
  );
}
