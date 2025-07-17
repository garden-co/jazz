"use client";

import { Button } from "@/components/atoms/Button";
import { Card } from "@/components/atoms/Card";
import { Switch } from "@/components/atoms/Switch";
import { JazzLogo } from "@/components/atoms/logos/JazzLogo";
import {
  Dropdown,
  DropdownButton,
  DropdownItem,
  DropdownMenu,
} from "@/components/organisms/Dropdown";
import { useState } from "react";
import {
  Style,
  glassContentClasses,
  styleToGlassMap,
  styleToGlassSubtleMap,
} from "../../../utils/tailwindClassesMap";

export default function Components() {
  const [checked, setChecked] = useState({
    md: true,
    sm: true,
  });
  const [selectedVariant, setSelectedVariant] = useState<Style>("primary");

  return (
    <div className="p-3 space-y-8">
      <div className="pb-4 flex gap-6 flex-col md:flex-row">
        <h3 className="text-md font-semibold">Switches</h3>
        <Switch
          label="Switch default (md) (Primary)"
          id="switch-md"
          checked={checked.md}
          onChange={() => setChecked({ ...checked, md: !checked.md })}
        />
        <Switch
          label="Switch (sm) success"
          id="switch-sm"
          checked={checked.sm}
          onChange={() => setChecked({ ...checked, sm: !checked.sm })}
          size="sm"
          intent="success"
        />
      </div>

      <div>
        <h3 className="text-lg font-bold mb-4">Enhanced Glass Effects</h3>
        <p className="mb-6 text-stone-600 dark:text-stone-400">
          Professional glass morphism effects with proper layering, specular
          highlights, and backdrop filters.
        </p>

        <div className="mb-8">
          <h4 className="text-md font-semibold mb-3">Glass Defraction Demo</h4>
          <p className="text-sm text-stone-600 dark:text-stone-400 mb-4">
            Design system Buttons with glass effects and Jazz logo scrolling
            underneath to demonstrate refraction.
          </p>

          <div className="relative h-80 rounded-xl overflow-hidden bg-gradient-to-br from-blue-50 via-purple-50 to-pink-50 dark:from-blue-900 dark:via-purple-900 dark:to-pink-900">
            {/* Scrollable content container */}
            <div className="h-full overflow-y-auto">
              <div className="h-[200%] flex flex-col items-center justify-start pt-16 space-y-12 pointer-events-none">
                {/* Jazz Logo */}
                <JazzLogo className="w-48 mt-56 h-auto opacity-90" />

                {/* Main text */}
                <div className="text-3xl font-bold text-stone-700 dark:text-stone-200">
                  Whip up an app
                </div>

                {/* Decorative elements */}
                {/* <div className="grid grid-cols-3 gap-6">
                  <div className="w-20 h-20 bg-gradient-to-br from-blue-400 to-blue-600 rounded-xl opacity-80 shadow-lg"></div>
                  <div className="w-20 h-20 bg-gradient-to-br from-purple-400 to-purple-600 rounded-xl opacity-80 shadow-lg"></div>
                  <div className="w-20 h-20 bg-gradient-to-br from-pink-400 to-pink-600 rounded-xl opacity-80 shadow-lg"></div>
                </div> */}

                <div className="text-stone-600 dark:text-stone-300 text-center max-w-md text-lg leading-relaxed">
                  Build real-time collaborative apps with local-first sync and
                  instant multiplayer
                </div>

                {/* Additional content to demonstrate scrolling */}
                {/* <div className="space-y-8 text-center">
                  <div className="text-xl font-semibold text-stone-700 dark:text-stone-200">
                    Features
                  </div>
                  <div className="grid grid-cols-2 gap-4 max-w-md">
                    <div className="p-4 bg-white/20 dark:bg-black/20 rounded-lg">
                      <div className="text-sm font-medium">Real-time Sync</div>
                    </div>
                    <div className="p-4 bg-white/20 dark:bg-black/20 rounded-lg">
                      <div className="text-sm font-medium">Local-first</div>
                    </div>
                    <div className="p-4 bg-white/20 dark:bg-black/20 rounded-lg">
                      <div className="text-sm font-medium">Multiplayer</div>
                    </div>
                    <div className="p-4 bg-white/20 dark:bg-black/20 rounded-lg">
                      <div className="text-sm font-medium">TypeScript</div>
                    </div>
                  </div>
                </div> */}
              </div>
            </div>

            {/* Sticky glass nav buttons using design system Buttons */}
            <div className="absolute bottom-6 left-1/2 transform -translate-x-1/2 z-10">
              <div className="flex items-center gap-3 p-3 rounded-2xl backdrop-blur-sm bg-white/5 dark:bg-black/5 shadow-lg">
                <Button intent="primary" variant="glass" size="sm">
                  Home
                </Button>
                <Button intent="success" variant="glass" size="sm">
                  Docs
                </Button>
                <Button intent="warning" variant="glass" size="sm">
                  Examples
                </Button>
                <Button intent="info" variant="glass" size="sm">
                  GitHub
                </Button>
              </div>
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          <div>
            <h4 className="text-md font-semibold mb-3">
              Buttons with Glass Effects
            </h4>
            <div className="space-y-3">
              <Button intent="primary" variant="glass">
                Primary Glass
              </Button>
              <Button intent="success" variant="glass">
                Success Glass
              </Button>
              <Button intent="danger" variant="glass">
                Danger Glass
              </Button>
            </div>
          </div>

          <div>
            <h4 className="text-md font-semibold mb-3">Subtle Glass Effects</h4>
            <div className="space-y-3">
              <Button
                className={styleToGlassSubtleMap.info}
                intent="info"
                variant="ghost"
              >
                <span className={glassContentClasses}>Subtle Info</span>
              </Button>
              <Button
                className={styleToGlassSubtleMap.warning}
                intent="warning"
                variant="ghost"
              >
                <span className={glassContentClasses}>Subtle Warning</span>
              </Button>
              <Button
                className={styleToGlassSubtleMap.tip}
                intent="tip"
                variant="ghost"
              >
                <span className={glassContentClasses}>Subtle Tip</span>
              </Button>
            </div>
          </div>

          <div>
            <h4 className="text-md font-semibold mb-3">
              Interactive Glass Elements
            </h4>
            <div className="space-y-3">
              <Dropdown>
                <DropdownButton
                  className={`w-full justify-between ${styleToGlassMap.primary}`}
                  intent="primary"
                >
                  <span className={glassContentClasses}>Glass Dropdown</span>
                </DropdownButton>
                <DropdownMenu
                  className={`${styleToGlassSubtleMap.default} border-0`}
                >
                  <DropdownItem onClick={() => setSelectedVariant("primary")}>
                    <span className={glassContentClasses}>primary</span>
                  </DropdownItem>
                  <DropdownItem onClick={() => setSelectedVariant("success")}>
                    <span className={glassContentClasses}>success</span>
                  </DropdownItem>
                  <DropdownItem onClick={() => setSelectedVariant("info")}>
                    <span className={glassContentClasses}>info</span>
                  </DropdownItem>
                </DropdownMenu>
              </Dropdown>

              <Button
                className={`w-full ${styleToGlassSubtleMap.warning}`}
                intent="warning"
                variant="ghost"
              >
                <span className={glassContentClasses}>Glass Ghost Button</span>
              </Button>
            </div>
          </div>
        </div>

        <div className="mt-8">
          <h4 className="text-md font-semibold mb-3">Premium Glass Cards</h4>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <Card className={styleToGlassMap.default}>
              <div className={`p-6 ${glassContentClasses}`}>
                <h5 className="font-semibold text-stone-800 dark:text-white mb-2">
                  Professional Glass Effect
                </h5>
                <p className="text-sm text-stone-600 dark:text-stone-300">
                  Multi-layer glass with backdrop blur, color overlay, and
                  specular highlights for premium appearance.
                </p>
              </div>
            </Card>

            <Card className={styleToGlassMap.success}>
              <div className={`p-6 ${glassContentClasses}`}>
                <h5 className="font-semibold text-stone-800 dark:text-white mb-2">
                  Colored Glass Premium
                </h5>
                <p className="text-sm text-stone-600 dark:text-stone-300">
                  Success-tinted glass with enhanced saturation and brightness
                  filters.
                </p>
              </div>
            </Card>
          </div>
        </div>

        <div className="mt-8">
          <h4 className="text-md font-semibold mb-3">Glass Effect Showcase</h4>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {(["primary", "warning", "info"] as Style[]).map((variant) => (
              <Card key={variant} className={styleToGlassMap[variant]}>
                <div className={`p-4 text-center ${glassContentClasses}`}>
                  <div
                    className={`w-12 h-12 mx-auto mb-2 rounded-full ${variant === "primary" ? "bg-blue-500/20" : variant === "warning" ? "bg-orange-500/20" : "bg-purple-500/20"} flex items-center justify-center`}
                  >
                    <span className="text-2xl">✨</span>
                  </div>
                  <h6 className="font-medium text-stone-800 dark:text-white capitalize mb-1">
                    {variant} Glass
                  </h6>
                  <p className="text-xs text-stone-600 dark:text-stone-300">
                    Professional glass morphism
                  </p>
                </div>
              </Card>
            ))}
          </div>
        </div>

        <div className="mt-8 p-6 bg-stone-50 dark:bg-stone-900 rounded-lg">
          <h4 className="text-md font-semibold mb-3">
            Glass Effect Architecture
          </h4>
          <p className="text-sm text-stone-600 dark:text-stone-400 mb-4">
            Based on the CodePen glass effect with proper layering and text
            accessibility:
          </p>

          <div className="space-y-3 text-xs">
            <div className="bg-stone-200 dark:bg-stone-800 p-3 rounded">
              <div className="font-semibold mb-1">Layer Structure:</div>
              <div className="space-y-1 ml-2">
                <div>
                  •{" "}
                  <code>
                    backdrop-blur-md [filter:saturate(125%)_brightness(1.1)]
                  </code>{" "}
                  - Enhanced glass filter
                </div>
                <div>
                  • <code>before:</code> - Gradient color overlay (z-1)
                </div>
                <div>
                  • <code>after:</code> - Refined specular highlights (z-2)
                </div>
                <div>
                  • <code>glassContentClasses</code> - Content with text shadow
                  (z-3)
                </div>
              </div>
            </div>

            <div className="bg-stone-200 dark:bg-stone-800 p-3 rounded">
              <div className="font-semibold mb-1">Improvements:</div>
              <div className="space-y-1 ml-2">
                <div>
                  • Better border alignment with <code>border-white/20</code>
                </div>
                <div>• Enhanced shadow depth and blur</div>
                <div>• Gradient overlays for richer color effects</div>
                <div>• Text shadows for better readability</div>
              </div>
            </div>

            <div className="bg-stone-200 dark:bg-stone-800 p-3 rounded">
              <div className="font-semibold mb-1">Usage:</div>
              <code className="block">
                className={`\${styleToGlassMap.primary}`}
              </code>
              <code className="block mt-1">
                className={`\${styleToGlassSubtleMap.success}`}
              </code>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
