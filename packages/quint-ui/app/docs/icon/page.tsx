import {
  BellIcon,
  CheckIcon,
  HeartIcon,
  HomeIcon,
  MailIcon,
  PlusIcon,
  SearchIcon,
  SettingsIcon,
  StarIcon,
  TrashIcon,
  UserIcon,
  XIcon,
} from "lucide-react";
import React from "react";
import { Icon } from "../../../src/components/icon";

export default function IconPage() {
  return (
    <div className="space-y-8 p-6 max-w-4xl mx-auto">
      <div>
        <h1 className="text-3xl font-bold text-stone-900 dark:text-white mb-2">
          Icon Component
        </h1>
        <p className="text-stone-600 dark:text-stone-400">
          A flexible icon wrapper that provides consistent styling for any
          Lucide icon or custom SVG. Import only the icons you need for optimal
          tree-shaking.
        </p>
      </div>

      {/* Import Pattern */}
      <section className="space-y-4">
        <div>
          <h2 className="text-xl font-semibold text-stone-900 dark:text-white">
            Import Pattern
          </h2>
          <p className="text-stone-600 dark:text-stone-400">
            Import icons individually from Lucide React for optimal bundle size
            and tree-shaking.
          </p>
        </div>
        <pre className="bg-stone-100 dark:bg-stone-800 p-4 rounded-lg text-sm overflow-x-auto">
          {`// Import Icon component and individual icons
import { HeartIcon, PlusIcon, TrashIcon } from "lucide-react";
import { Icon } from "@/components/icon";

<Icon intent="primary" size="lg">
  <HeartIcon />
</Icon>`}
        </pre>
      </section>

      <section className="space-y-4">
        <div></div>
        <div className="flex flex-wrap gap-4 p-4 bg-stone-50 dark:bg-stone-900 rounded-lg">
          <Icon intent="primary" size="lg">
            <HeartIcon />
          </Icon>
          <Icon intent="success" size="lg">
            <CheckIcon />
          </Icon>
          <Icon intent="warning" size="lg">
            <StarIcon />
          </Icon>
          <Icon intent="danger" size="lg">
            <TrashIcon />
          </Icon>
          <Icon intent="info" size="lg">
            <BellIcon />
          </Icon>
        </div>
        <pre className="bg-stone-100 dark:bg-stone-800 p-4 rounded-lg text-sm overflow-x-auto">
          {`<Icon intent="primary" size="lg">
  <HeartIcon />
</Icon>`}
        </pre>
      </section>

      {/* Size Variants */}
      <section className="space-y-4">
        <div>
          <h2 className="text-xl font-semibold text-stone-900 dark:text-white">
            Size Variants
          </h2>
          <p className="text-stone-600 dark:text-stone-400">
            Available sizes from 2xs to xl with automatic stroke width
            adjustment.
          </p>
        </div>
        <div className="flex items-center gap-4 p-4 bg-stone-50 dark:bg-stone-900 rounded-lg">
          <Icon intent="primary" size="2xs">
            <HeartIcon />
          </Icon>
          <Icon intent="primary" size="xs">
            <HeartIcon />
          </Icon>
          <Icon intent="primary" size="sm">
            <HeartIcon />
          </Icon>
          <Icon intent="primary" size="md">
            <HeartIcon />
          </Icon>
          <Icon intent="primary" size="lg">
            <HeartIcon />
          </Icon>
          <Icon intent="primary" size="xl">
            <HeartIcon />
          </Icon>
        </div>
        <pre className="bg-stone-100 dark:bg-stone-800 p-4 rounded-lg text-sm overflow-x-auto">
          {`<Icon intent="primary" size="2xs"><HeartIcon /></Icon>
<Icon intent="primary" size="xs"><HeartIcon /></Icon>
<Icon intent="primary" size="sm"><HeartIcon /></Icon>
<Icon intent="primary" size="md"><HeartIcon /></Icon>
<Icon intent="primary" size="lg"><HeartIcon /></Icon>
<Icon intent="primary" size="xl"><HeartIcon /></Icon>`}
        </pre>
      </section>

      {/* Intent Variants */}
      <section className="space-y-4">
        <div>
          <h2 className="text-xl font-semibold text-stone-900 dark:text-white">
            Intent Variants
          </h2>
          <p className="text-stone-600 dark:text-stone-400">
            Different color intents for various contexts and states.
          </p>
        </div>
        <div className="grid grid-cols-4 gap-4 p-4 bg-stone-50 dark:bg-stone-900 rounded-lg">
          <div className="flex flex-col items-center gap-2">
            <Icon intent="default" size="lg">
              <HomeIcon />
            </Icon>
            <span className="text-xs text-stone-600 dark:text-stone-400">
              default
            </span>
          </div>
          <div className="flex flex-col items-center gap-2">
            <Icon intent="primary" size="lg">
              <StarIcon />
            </Icon>
            <span className="text-xs text-stone-600 dark:text-stone-400">
              primary
            </span>
          </div>
          <div className="flex flex-col items-center gap-2">
            <Icon intent="success" size="lg">
              <CheckIcon />
            </Icon>
            <span className="text-xs text-stone-600 dark:text-stone-400">
              success
            </span>
          </div>
          <div className="flex flex-col items-center gap-2">
            <Icon intent="warning" size="lg">
              <BellIcon />
            </Icon>
            <span className="text-xs text-stone-600 dark:text-stone-400">
              warning
            </span>
          </div>
          <div className="flex flex-col items-center gap-2">
            <Icon intent="danger" size="lg">
              <XIcon />
            </Icon>
            <span className="text-xs text-stone-600 dark:text-stone-400">
              danger
            </span>
          </div>
          <div className="flex flex-col items-center gap-2">
            <Icon intent="info" size="lg">
              <MailIcon />
            </Icon>
            <span className="text-xs text-stone-600 dark:text-stone-400">
              info
            </span>
          </div>
          <div className="flex flex-col items-center gap-2">
            <Icon intent="muted" size="lg">
              <SettingsIcon />
            </Icon>
            <span className="text-xs text-stone-600 dark:text-stone-400">
              muted
            </span>
          </div>
          <div className="flex flex-col items-center gap-2">
            <Icon intent="strong" size="lg">
              <UserIcon />
            </Icon>
            <span className="text-xs text-stone-600 dark:text-stone-400">
              strong
            </span>
          </div>
        </div>
        <pre className="bg-stone-100 dark:bg-stone-800 p-4 rounded-lg text-sm overflow-x-auto">
          {`<Icon intent="primary"><StarIcon /></Icon>
<Icon intent="success"><CheckIcon /></Icon>
<Icon intent="warning"><BellIcon /></Icon>
<Icon intent="danger"><XIcon /></Icon>`}
        </pre>
      </section>

      {/* With Backgrounds */}
      <section className="space-y-4">
        <div>
          <h2 className="text-xl font-semibold text-stone-900 dark:text-white">
            Background & Hover Effects
          </h2>
          <p className="text-stone-600 dark:text-stone-400">
            Add backgrounds and hover effects for interactive elements.
          </p>
        </div>
        <div className="flex flex-wrap gap-4 p-4 bg-stone-50 dark:bg-stone-900 rounded-lg">
          <Icon intent="primary" hasBackground hasHover size="lg">
            <HeartIcon />
          </Icon>
          <Icon intent="success" hasBackground hasHover size="lg">
            <CheckIcon />
          </Icon>
          <Icon intent="warning" hasBackground hasHover size="lg">
            <StarIcon />
          </Icon>
          <Icon intent="danger" hasBackground hasHover size="lg">
            <TrashIcon />
          </Icon>
          <Icon intent="info" hasBackground hasHover size="lg">
            <BellIcon />
          </Icon>
        </div>
        <pre className="bg-stone-100 dark:bg-stone-800 p-4 rounded-lg text-sm overflow-x-auto">
          {`<Icon intent="primary" hasBackground hasHover size="lg">
  <HeartIcon />
</Icon>`}
        </pre>
      </section>

      {/* Custom SVG */}
      <section className="space-y-4">
        <div>
          <h2 className="text-xl font-semibold text-stone-900 dark:text-white">
            Custom SVG Icons
          </h2>
          <p className="text-stone-600 dark:text-stone-400">
            Use custom SVG icons while maintaining consistent styling.
          </p>
        </div>
        <div className="flex flex-wrap gap-4 p-4 bg-stone-50 dark:bg-stone-900 rounded-lg">
          <Icon intent="primary" size="lg">
            <svg viewBox="0 0 24 24" fill="currentColor">
              <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z" />
            </svg>
          </Icon>
          <Icon intent="danger" size="lg">
            <svg viewBox="0 0 24 24" fill="currentColor">
              <path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z" />
            </svg>
          </Icon>
        </div>
        <pre className="bg-stone-100 dark:bg-stone-800 p-4 rounded-lg text-sm overflow-x-auto">
          {`<Icon intent="primary" size="lg">
  <svg viewBox="0 0 24 24" fill="currentColor">
    <path d="M12 2l3.09 6.26L22 9.27l-5 4.87..."/>
  </svg>
</Icon>`}
        </pre>
      </section>

      {/* Usage in Components */}
      <section className="space-y-4">
        <div>
          <h2 className="text-xl font-semibold text-stone-900 dark:text-white">
            Usage in Components
          </h2>
          <p className="text-stone-600 dark:text-stone-400">
            Examples of how to use icons within other components like buttons.
          </p>
        </div>
        <div className="flex flex-wrap gap-4 p-4 bg-stone-50 dark:bg-stone-900 rounded-lg">
          <button className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700">
            <Icon intent="white" size="sm">
              <PlusIcon />
            </Icon>
            Add Item
          </button>
          <button className="flex items-center gap-2 px-4 py-2 bg-stone-100 dark:bg-stone-800 text-stone-900 dark:text-white rounded-lg hover:bg-stone-200 dark:hover:bg-stone-700">
            <Icon intent="muted" size="sm">
              <SearchIcon />
            </Icon>
            Search
          </button>
          <button className="flex items-center gap-2 px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700">
            <Icon intent="white" size="sm">
              <TrashIcon />
            </Icon>
            Delete
          </button>
        </div>
        <pre className="bg-stone-100 dark:bg-stone-800 p-4 rounded-lg text-sm overflow-x-auto">
          {`<button className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg">
  <Icon intent="white" size="sm">
    <PlusIcon />
  </Icon>
  Add Item
</button>`}
        </pre>
      </section>

      {/* API Reference */}
      <section className="space-y-4">
        <div>
          <h2 className="text-xl font-semibold text-stone-900 dark:text-white">
            API Reference
          </h2>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full border-collapse border border-stone-300 dark:border-stone-700">
            <thead>
              <tr className="bg-stone-100 dark:bg-stone-800">
                <th className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-left">
                  Prop
                </th>
                <th className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-left">
                  Type
                </th>
                <th className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-left">
                  Default
                </th>
                <th className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-left">
                  Description
                </th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 font-mono text-sm">
                  children
                </td>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-sm">
                  ReactNode
                </td>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-sm">
                  -
                </td>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-sm">
                  Any Lucide icon or custom SVG (preferred method)
                </td>
              </tr>
              <tr>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 font-mono text-sm">
                  icon
                </td>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-sm">
                  LucideIcon
                </td>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-sm">
                  -
                </td>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-sm">
                  Direct icon component prop
                </td>
              </tr>
              <tr>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 font-mono text-sm">
                  size
                </td>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-sm">
                  "2xs" | "xs" | "sm" | "md" | "lg" | "xl"
                </td>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-sm">
                  "md"
                </td>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-sm">
                  Icon size
                </td>
              </tr>
              <tr>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 font-mono text-sm">
                  intent
                </td>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-sm">
                  "default" | "primary" | "success" | "warning" | "danger" |
                  "info" | "muted" | "strong" | "white"
                </td>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-sm">
                  "default"
                </td>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-sm">
                  Color intent
                </td>
              </tr>
              <tr>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 font-mono text-sm">
                  hasBackground
                </td>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-sm">
                  boolean
                </td>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-sm">
                  false
                </td>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-sm">
                  Add background color
                </td>
              </tr>
              <tr>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 font-mono text-sm">
                  hasHover
                </td>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-sm">
                  boolean
                </td>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-sm">
                  false
                </td>
                <td className="border border-stone-300 dark:border-stone-700 px-4 py-2 text-sm">
                  Enable hover effects
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
