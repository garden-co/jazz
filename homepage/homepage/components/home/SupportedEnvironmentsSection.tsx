import { BunLogo } from "@/components/icons/BunLogo";
import { CloudflareWorkerLogo } from "@/components/icons/CloudflareWorkerLogo";
import { ExpoLogo } from "@/components/icons/ExpoLogo";
import { JavascriptLogo } from "@/components/icons/JavascriptLogo";
import { NodejsLogo } from "@/components/icons/NodejsLogo";
import { ReactLogo } from "@/components/icons/ReactLogo";
import { ReactNativeLogo } from "@/components/icons/ReactNativeLogo";
import { SvelteLogo } from "@/components/icons/SvelteLogo";
import { GappedGrid } from "@garden-co/design-system/src/components/molecules/GappedGrid";
import React from "react";

export function SupportedEnvironmentsSection() {
  const frameworks = [
    {
      name: "JavaScript",
      icon: JavascriptLogo,
    },
    {
      name: "React",
      icon: ReactLogo,
    },
    {
      name: "React Native",
      icon: ReactNativeLogo,
    },
    {
      name: "Expo",
      icon: ExpoLogo,
    },
    {
      name: "Svelte",
      icon: SvelteLogo,
    },
  ];

  const serverWorkers = [
    {
      name: "Node.js",
      icon: NodejsLogo,
    },
    {
      name: "Cloudflare Workers",
      icon: CloudflareWorkerLogo,
    },
    {
      name: "Bun",
      icon: BunLogo,
    },
  ];

  const first = [
    {
      name: "JavaScript",
      icon: JavascriptLogo,
    },
    {
      name: "React",
      icon: ReactLogo,
    },
    {
      name: "Svelte",
      icon: SvelteLogo,
    },
    {
      name: "Expo",
      icon: ExpoLogo,
    },
    {
      name: "React Native",
      icon: ReactNativeLogo,
    },
  ];

  const second = [
    {
      name: "Node.js",
      icon: NodejsLogo,
    },
    {
      name: "Cloudflare Workers",
      icon: CloudflareWorkerLogo,
    },
    {
      name: "Bun",
      icon: BunLogo,
    },
  ];

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
            <div className="flex gap-x-6 gap-y-3 grayscale flex-col lg:flex-row">
              {items.map(({ name, icon: Icon }) => (
                <div key={name} className="flex items-center gap-2">
                  <Icon className="size-6" />
                  {name}
                </div>
              ))}
            </div>
          </div>
        ))}
      </GappedGrid>
    </>
  );
}
