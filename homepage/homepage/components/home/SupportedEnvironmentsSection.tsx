import { KotlinLogo } from "@/components/icons/KotlinLogo";
import { NodejsLogo } from "@/components/icons/NodejsLogo";
import { ReactLogo } from "@/components/icons/ReactLogo";
import { ReactNativeLogo } from "@/components/icons/ReactNativeLogo";
import { RustLogo } from "@/components/icons/RustLogo";
import { SvelteLogo } from "@/components/icons/SvelteLogo";
import { SwiftLogo } from "@/components/icons/SwiftLogo";
import { VueLogo } from "@/components/icons/VueLogo";
import { GlobeIcon } from "lucide-react";
import React from "react";

export function SupportedEnvironmentsSection() {
  const supported = [
    {
      name: "Browser (vanilla JS)",
      icon: (
        <GlobeIcon
          strokeWidth={1}
          className="text-stone-900 dark:text-white"
          height="1em"
          width="1em"
        />
      ),
    },
    {
      name: "React",
      icon: <ReactLogo />,
    },
    {
      name: "React Native",
      icon: <ReactNativeLogo />,
    },
    {
      name: "Node.js",
      icon: <NodejsLogo />,
    },
  ];

  const comingSoon = [
    {
      name: "Vue",
      icon: <VueLogo />,
    },
    {
      name: "Svelte",
      icon: <SvelteLogo />,
    },
    {
      name: "Swift",
      icon: <SwiftLogo />,
    },
    {
      name: "Rust",
      icon: <RustLogo className="text-black dark:text-white" />,
    },
    {
      name: "Kotlin",
      icon: <KotlinLogo />,
    },
  ];

  return (
    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3 lg:gap-8">
      <h2 className="font-semibold tracking-tight font-display text-2xl text-stone-900 lg:text-balance sm:text-4xl dark:text-white">
        Jazz works with your favorite stack
      </h2>
      <div className="flex flex-col gap-6 lg:col-span-2 lg:gap-8">
        <div className="flex flex-col gap-5 lg:flex-row lg:gap-3">
          {supported.map((tech) => (
            <div
              key={tech.name}
              className="flex items-center gap-2 lg:py-3 lg:px-4 lg:shadow-sm rounded-lg lg:border"
            >
              <span className="text-2xl">{tech.icon}</span>
              <div className="text-center font-medium text-stone-900 dark:text-white">
                {tech.name}
              </div>
            </div>
          ))}
        </div>
        <div className="flex flex-col gap-3">
          <p className="text-sm">Coming soon</p>
          <div className="flex gap-x-5 gap-y-3 flex-wrap">
            {comingSoon.map((tech) => (
              <div key={tech.name} className="flex items-center gap-2">
                <span className="text-xl">{tech.icon}</span>
                <div className="text-center text-sm text-stone-900 dark:text-white">
                  {tech.name}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
