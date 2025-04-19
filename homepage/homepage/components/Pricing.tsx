import { clsx } from "clsx";
import { Button } from "@garden-co/design-system/src/components/atoms/Button";
import {
  CircleCheckIcon,
  LucideBuilding2,
  LucideChevronUp,
  LucideChevronsUp,
  LucideCloudDownload,
  LucideDatabase,
  LucideHandshake,
  LucideIcon,
  LucideInfinity,
  LucideServer,
  LucideUsers,
} from "lucide-react";
import { FakeGetStartedButton } from "./FakeGetStartedButton";
import { IndieTierLogo, ProTierLogo, StarterTierLogo } from "./TierLogos";

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
    gray: <Icon size={iconSize} className="text-stone-500 shrink-0 mt-1" />,
    blue: (
      <Icon
        size={iconSize}
        className="text-primary dark:text-white shrink-0 mt-1"
      />
    ),
  };

  return (
    <li
      className={clsx(
        "inline-flex gap-3 text-stone-800 dark:text-stone-200 py-2",
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
      <div className="flex flex-col sm:max-w-lg mx-auto md:max-w-none md:flex-row md:items-start gap-4 mb-10">
        <div className="flex-1 flex flex-col gap-3 overflow-hidden rounded-xl p-6 shadow-lg shadow-gray-900/5 bg-white dark:bg-stone-925">
          <h3 className="flex justify-between items-center font-semibold text-stone-900 text-xl dark:text-white">
            <div className="flex items-center gap-1.5">
              <StarterTierLogo />
              Starter
            </div>
            <div className="text-highlight">
              <span className="text-2xl font-light tabular-nums tracking-tighter">
                $0
              </span>
              <span className="text-sm font-normal text-stone-600 dark:text-stone-500">
                /mo
              </span>
            </div>
          </h3>

          <p className="text-sm">Everything you need to get started.</p>

          <div>
            <ul className="flex flex-col my-4 text-sm lg:text-base">
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
          </div>

          <FakeGetStartedButton tier="starter" />

          <p className="text-sm">No credit card required. Takes 20s.</p>
        </div>
        <div className="flex-1 flex flex-col gap-3 overflow-hidden rounded-xl p-6 shadow-lg shadow-gray-900/5 bg-white dark:bg-stone-925">
          <div>
            <h3 className="flex justify-between items-center font-semibold text-stone-900 text-xl dark:text-white">
              <div className="flex items-center gap-1.5">
                <IndieTierLogo />
                Indie
              </div>
              <div className="text-highlight">
                <span className="text-2xl font-light tabular-nums tracking-tighter">
                  $4
                </span>
                <span className="text-sm font-normal text-stone-600 dark:text-stone-500">
                  /mo
                </span>
              </div>
            </h3>
          </div>

          <p className="text-sm">
            Get robust real-time infra at predictable costs.
          </p>

          <div>
            <ul className="flex flex-col my-4 text-sm lg:text-base">
              <ListItem icon={LucideUsers}>
                <span className="tabular-nums">10,000</span> monthly active
                users
              </ListItem>
              <ListItem icon={LucideDatabase}>
                <span className="tabular-nums">100</span> GB storage incl.{" "}
                <span className="text-sm">(then $0.02 per GB)</span>
              </ListItem>
              <ListItem icon={LucideCloudDownload}>
                <span className="tabular-nums">20</span> GB egress/mo incl.{" "}
                <span className="text-sm">(then $0.1 per GB)</span>
              </ListItem>
              <hr className="my-2 border" />
              <ListItem icon={LucideChevronsUp}>High-priority sync</ListItem>
            </ul>
          </div>

          <FakeGetStartedButton tier="indie" />

          <p className="text-sm">
            One month free trial. Unlimited projects. Takes 1min.
          </p>
        </div>
        <div className="flex-1 flex flex-col gap-3 overflow-hidden rounded-xl p-6 shadow-lg shadow-gray-900/5 bg-white dark:bg-stone-925 pb-6">
          <h3 className="flex justify-between items-center font-semibold text-stone-900 text-xl dark:text-white">
            <div className="flex items-center gap-1.5">
              <ProTierLogo />
              Pro
            </div>
            <div className="text-highlight">
              <span className="text-lg font-normal">from</span>{" "}
              <span className="text-2xl font-light tabular-nums tracking-tighter">
                $199
              </span>
              <span className="text-sm font-normal text-stone-600 dark:text-stone-500">
                /mo
              </span>
            </div>
          </h3>

          <p className="text-sm">
            Get our best infra and move quickly with our help.
          </p>

          <div>
            <ul className="flex flex-col my-4 text-sm lg:text-base">
              <ListItem icon={LucideUsers}>
                Custom monthly active users
              </ListItem>
              <ListItem icon={LucideDatabase}>Custom storage</ListItem>
              <ListItem icon={LucideCloudDownload}>Custom egress/mo</ListItem>
              <hr className="my-2 border" />
              <ListItem icon={LucideHandshake}>
                Rapid integration & premium onboarding
              </ListItem>
              <ListItem icon={LucideBuilding2}>
                SLAs, certifications, dedicated support
              </ListItem>
              <ListItem icon={LucideServer}>
                Dedicated / on-prem infrastructure
              </ListItem>
            </ul>
          </div>

          <Button href="https://cal.com/anselm-io/cloud-pro-intro">
            Schedule intro call
          </Button>

          <p className="text-sm">
            Our engineering team will get you going for free.
          </p>
        </div>
      </div>
      {/* <H3>Add-ons</H3>
      <div className="flex flex-col sm:max-w-lg mx-auto md:max-w-none md:flex-row md:items-start gap-4 mb-10">
        <div className="md:flex-[2] flex flex-col gap-1 overflow-hidden rounded-xl px-6 py-4 shadow-lg shadow-gray-900/5 bg-white dark:bg-stone-925">
          <h4 className="flex justify-between items-center font-semibold text-stone-900 text-lg dark:text-white">
            <div className="flex items-center gap-1.5">Simple Analytics</div>
            <div className="text-highlight">
              <span className="text-xl font-light tabular-nums tracking-tighter">
                $10
              </span>
              <span className="text-sm font-normal text-stone-600 dark:text-stone-500">
                /mo
              </span>
            </div>
          </h4>
          <div>
            <ul className="flex flex-col text-sm lg:text-base">
              <ListItem icon={LucideScanSearch}>
                <span className="tabular-nums">100</span> inspected users{" "}
                <span className="text-sm text-highlight">
                  (then $0.01 per user)
                </span>
              </ListItem>
            </ul>
          </div>
          <p className="text-sm">
            You can choose which subset of users to inspect.
          </p>
        </div>
        <div className="md:flex-[2] flex flex-col gap-1 overflow-hidden rounded-xl px-6 py-4 shadow-lg shadow-gray-900/5 bg-white dark:bg-stone-925">
          <h4 className="flex justify-between items-center font-semibold text-stone-900 text-lg dark:text-white">
            <div className="flex items-center gap-1.5">Advanced Analytics</div>
            <div className="text-highlight">
              <span className="text-xl font-light tabular-nums tracking-tighter">
                $50
              </span>
              <span className="text-sm font-normal text-stone-600 dark:text-stone-500">
                /mo
              </span>
            </div>
          </h4>
          <div>
            <ul className="flex flex-col text-sm lg:text-base">
              <ListItem icon={LucideMicroscope}>
                <span className="tabular-nums">1,000</span> inspected users{" "}
                <span className="text-sm text-highlight">
                  (then $0.05 per user)
                </span>
              </ListItem>
            </ul>
          </div>
          <p className="text-sm">
            You can choose which subset of users to inspect.
          </p>
        </div>
        <div className="md:flex-[2] flex flex-col gap-1 overflow-hidden rounded-xl px-6 py-4 shadow-lg shadow-gray-900/5 bg-white dark:bg-stone-925">
          <h4 className="flex justify-between items-center font-semibold text-stone-900 text-lg dark:text-white">
            <div className="flex items-center gap-1.5">Custom Sync Domain</div>
            <div className="text-highlight">
              <span className="text-xl font-light tabular-nums tracking-tighter">
                $50
              </span>
              <span className="text-sm font-normal text-stone-600 dark:text-stone-500">
                /mo
              </span>
            </div>
          </h4>
          <div>
            <ul className="flex flex-col text-sm lg:text-base">
              <ListItem icon={LucideGlobe}>
                Sync via <code>sync.yourdomain.com</code>
              </ListItem>
            </ul>
          </div>
          <p className="text-sm">
            White-labeled & friendly with complex CORS setups.
          </p>
        </div>
      </div> */}
    </>
  );
}
