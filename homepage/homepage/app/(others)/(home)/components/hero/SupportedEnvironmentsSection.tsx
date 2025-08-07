import { BunLogo } from "@/components/icons/BunLogo";
import { CloudflareWorkerLogo } from "@/components/icons/CloudflareWorkerLogo";
import { VercelLogo } from "@/components/icons/VercelLogo";
import { ExpoLogo } from "@/components/icons/ExpoLogo";
import { JavascriptLogo } from "@/components/icons/JavascriptLogo";
import { NodejsLogo } from "@/components/icons/NodejsLogo";
import { ReactLogo } from "@/components/icons/ReactLogo";
import { ReactNativeLogo } from "@/components/icons/ReactNativeLogo";
import { SvelteLogo } from "@/components/icons/SvelteLogo";
import { GappedGrid } from "@garden-co/design-system/src/components/molecules/GappedGrid";
import Link from "next/link";
import React from "react";

const frameworks = [
  {
    name: "JavaScript",
    icon: JavascriptLogo,
    href: "/docs/vanilla",
  },
  {
    name: "React",
    icon: ReactLogo,
    href: "/docs/react",
  },
  {
    name: "React Native",
    icon: ReactNativeLogo,
    href: "/docs/react-native",
  },
  {
    name: "Expo",
    icon: ExpoLogo,
    href: "/docs/react-native-expo",
  },
  {
    name: "Svelte",
    icon: SvelteLogo,
    href: "/docs/svelte",
  },
];

const serverWorkers = [
  {
    name: "Node.js",
    icon: NodejsLogo,
    href: "/docs/react/server-workers",
  },
  {
    name: "Bun",
    icon: BunLogo,
  },
  {
    name: "Vercel",
    icon: VercelLogo,
  },
  {
    name: "CF Workers",
    icon: CloudflareWorkerLogo,
  }
];

export function SupportedEnvironmentsSection() {
  return (
    <>
      <h2 className="sr-only">Supported environments</h2>
      <GappedGrid>
        {[
          {
            label: "Build apps with",
            items: frameworks,
          },
          {
            label: "Optionally add server workers",
            items: serverWorkers,
          },
        ].map(({ label, items }) => (
          <div className="col-span-2 lg:col-span-3" key={label}>
            <h3 className="mb-4 text-highlight font-medium">{label}</h3>
            <div className="flex gap-x-6 gap-y-3  flex-col lg:flex-row">
              {items.map(({ name, icon: Icon, href }) => {
                if (href) {
                  return (
                    <Link
                      href={href}
                      key={name}
                      className="flex items-center gap-2 grayscale hover:grayscale-0"
                    >
                      <Icon className="size-6" />
                      {name}
                    </Link>
                  );
                }
                return (
                  <div key={name} className="flex items-center gap-2 grayscale">
                    <Icon className="size-6" />
                    {name}
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </GappedGrid>
    </>
  );
}
