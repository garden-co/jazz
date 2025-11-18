import { BunLogo } from "@/components/icons/BunLogo";
import { CloudflareWorkerLogo } from "@/components/icons/CloudflareWorkerLogo";
import { VercelLogo } from "@/components/icons/VercelLogo";
import { ExpoLogo } from "@/components/icons/ExpoLogo";
import { JavascriptLogo } from "@/components/icons/JavascriptLogo";
import { NodejsLogo } from "@/components/icons/NodejsLogo";
import { ReactLogo } from "@/components/icons/ReactLogo";
import { ReactNativeLogo } from "@/components/icons/ReactNativeLogo";
import { SvelteLogo } from "@/components/icons/SvelteLogo";
import Link from "next/link";
import React from "react";

const environments = [
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
  {
    name: "Node.js",
    icon: NodejsLogo,
    href: "/docs/react/server-side/setup",
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
  },
];

export function SupportedEnvironmentsSection() {
  return (
    <div className="container flex flex-wrap justify-center gap-4 py-12 md:gap-6 lg:pt-24">
      {environments.map(({ name, icon: Icon, href }) => {
        if (href) {
          return (
            <Link
              href={href}
              key={name}
              className="flex items-center gap-2 grayscale hover:grayscale-0"
            >
              <Icon className="size-6" />
              <span className="hidden md:block">{name}</span>
            </Link>
          );
        }
        return (
          <div
            key={name}
            className="flex items-center justify-center gap-2 grayscale"
          >
            <Icon className="size-6" />
            <span className="hidden md:block">{name}</span>
          </div>
        );
      })}
    </div>
  );
}
