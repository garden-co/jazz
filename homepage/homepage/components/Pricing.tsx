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
} from "lucide-react";
import { IndieTierLogo, ProTierLogo, StarterTierLogo } from "./TierLogos";
import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import { H2 } from "@garden-co/design-system/src/components/atoms/Headings";

export function ListItem({
  variant = "blue",
  icon: Icon = CircleCheckIcon,
  className = "",
  children,
}: {
  variant?: "gray" | "blue";
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
      <H2>Pricing</H2>
      {/* Self-hosted option banner */}
      <div className="bg-linear-to-r shadow-xs rounded-xl border from-stone-50 to-gray-50 p-6 shadow-gray-900/5 dark:from-stone-925 dark:to-stone-950">
        <div className="flex items-start gap-4">
          <div className="grid w-full items-center gap-4 md:flex">
            <div className="grid grow gap-2">
              <h3 className="flex w-full items-center justify-between text-xl font-semibold text-stone-900 dark:text-white">
                <span className="flex items-center gap-1.5">
                  <Icon name="server" />
                  Self-Hosted
                </span>
              </h3>

              <p className="text-base text-stone-600 dark:text-stone-400">
                Self-host Jazz for complete control. From hybrid to fully
                private deployments — it's your infrastructure.
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

      <div className="mb-10 grid gap-4 md:grid-cols-3">
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
            <ListItem icon={LucideCloud}>Optimal cloud routing</ListItem>
            <ListItem icon={LucideDatabaseZap}>Smart caching</ListItem>
            <ListItem icon={LucideImagePlay}>
              Blob storage & media streaming
            </ListItem>
            <li aria-hidden="true" className="my-2 list-none border-t-2" />
            <ListItem icon={LucideUsers}>
              <span className="tabular-nums">100</span> monthly active users
            </ListItem>
            <ListItem icon={LucideDatabase}>
              <span className="tabular-nums">10</span> GB storage
            </ListItem>
            <ListItem icon={LucideCloudDownload}>
              <span className="tabular-nums">2</span> GB egress/mo
            </ListItem>
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

          <p className="text-sm">
            Get robust real-time infra at predictable costs.
          </p>

          <ul className="my-4 mb-auto flex w-full flex-col text-sm lg:text-base">
            <ListItem icon={LucideCloud}>Optimal cloud routing</ListItem>
            <ListItem icon={LucideDatabaseZap}>Smart caching</ListItem>
            <ListItem icon={LucideImagePlay}>
              Blob storage & media streaming
            </ListItem>
            <li aria-hidden="true" className="my-2 list-none border-t-2" />
            <ListItem icon={LucideUsers}>
              <span className="tabular-nums">10,000</span> monthly active users
            </ListItem>
            <ListItem icon={LucideDatabase}>
              <span className="tabular-nums">100</span> GB storage incl.{" "}
              <span className="text-sm">(then $0.02 per GB)</span>
            </ListItem>
            <ListItem icon={LucideCloudDownload}>
              <span className="tabular-nums">20</span> GB egress/mo incl.{" "}
              <span className="text-sm">(then $0.1 per GB)</span>
            </ListItem>
            <li aria-hidden="true" className="my-2 list-none border-t-2" />
            <ListItem icon={LucideChevronsUp}>High-priority sync</ListItem>
          </ul>

          <Button
            href="https://dashboard.jazz.tools?utm_source=cloud_cta_indie"
            newTab
            intent="primary"
          >
            Get Indie API key
          </Button>
          <p className="text-sm">
            One month free trial. Unlimited projects. Takes 1min.
          </p>
        </div>
        <div className="shadow-xs flex flex-col items-start gap-3 rounded-xl border bg-stone-100 p-6 shadow-gray-900/5 dark:bg-stone-925">
          <h3 className="flex w-full items-center justify-between text-xl font-semibold text-stone-900 dark:text-white">
            <span className="flex items-center gap-1.5">
              <ProTierLogo />
              Pro
            </span>
            <span className="text-highlight">
              <span className="text-lg font-normal">from</span>{" "}
              <span className="text-2xl font-light tabular-nums tracking-tighter">
                $199
              </span>
              <span className="text-sm font-normal text-stone-600 dark:text-stone-500">
                /mo
              </span>
            </span>
          </h3>

          <p className="text-sm">
            Get our best infra and move quickly with our help.
          </p>

          <ul className="my-4 flex w-full flex-col text-sm lg:text-base">
            <ListItem icon={LucideCloud}>Optimal cloud routing</ListItem>
            <ListItem icon={LucideDatabaseZap}>Smart caching</ListItem>
            <ListItem icon={LucideImagePlay}>
              Blob storage & media streaming
            </ListItem>
            <li aria-hidden="true" className="my-2 list-none border-t-2" />
            <ListItem icon={LucideUsers}>Custom monthly active users</ListItem>
            <ListItem icon={LucideDatabase}>Custom storage</ListItem>
            <ListItem icon={LucideCloudDownload}>Custom egress/mo</ListItem>
            <li aria-hidden="true" className="my-2 list-none border-t-2" />
            <ListItem icon={LucideHandshake}>
              Rapid integration &amp premium onboarding
            </ListItem>
            <ListItem icon={LucideBuilding2}>
              SLAs, certifications, dedicated support
            </ListItem>
            <ListItem icon={LucideServer}>
              Dedicated / on-prem infrastructure
            </ListItem>
          </ul>

          <Button
            href="https://cal.com/anselm-io/cloud-pro-intro"
            intent="primary"
          >
            Schedule intro call
          </Button>

          <p className="text-sm">
            Our engineering team will get you going for free.
          </p>
        </div>
      </div>
    </>
  );
}
