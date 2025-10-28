"use client";
import { Button } from "@/src/components/button";
import { ArrowRight, CloudHail, Rocket } from "lucide-react";
import { useState } from "react";

export default function ButtonDocsPage() {
  const variants = [
    "default",
    "primary",
    "tip",
    "info",
    "success",
    "warning",
    "alert",
    "danger",
    "muted",
    "strong",
  ] as const;
  const [selectedVariant, setSelectedVariant] =
    useState<(typeof variants)[number]>("primary");
  return (
    <>
      <h2 className="text-2xl mt-5 mb-2 font-bold">Buttons</h2>
      <h3 className="text-lg mt-5 mb-2 font-bold">Variants</h3>

      <p className="my-3">Buttons are styled with the variant prop.</p>

      <div className="grid grid-cols-2 gap-2">
        <Button variant="default">default</Button>
        <Button variant="inverted">inverted</Button>
        <Button variant="link">link</Button>
        <Button variant="ghost">ghost</Button>
        <Button variant="outline">outline</Button>
      </div>

      <h3 className="text-lg mt-5 font-bold">Intents</h3>
      <p className="my-3">
        We have extended the variants to include more styles via the intent
        prop.
      </p>

      <div className="grid grid-cols-2 gap-2">
        <Button intent="default">default</Button>
        <Button intent="muted">muted</Button>
        <Button intent="strong">strong</Button>
        <Button intent="primary">primary</Button>
        <Button intent="tip">tip</Button>
        <Button intent="info">info</Button>
        <Button intent="success">success</Button>
        <Button intent="warning">warning</Button>
        <Button intent="alert">alert</Button>
        <Button intent="danger">danger</Button>
      </div>

      <div className="flex justify-between items-center w-48 mt-10">
        <h3 className="text-lg font-bold min-w-52">Variants & Intents</h3>
      </div>

      <p className="text-sm mt-2 mb-5">
        <strong>NB:</strong> Variants and styles are interchangeable. See the
        intent on each variant with the dropdown.
      </p>

      <div className="grid grid-cols-2 gap-2">
        <Button intent={selectedVariant} variant="outline">
          outline
        </Button>
        <Button intent={selectedVariant} variant="inverted">
          inverted
        </Button>
        <Button intent={selectedVariant} variant="ghost">
          ghost
        </Button>
        <Button intent={selectedVariant} variant="link">
          link
        </Button>
      </div>

      <p className="my-3">
        For compatibility the shadcn/ui variants are mapped to the design
        system.
      </p>

      <div className="grid grid-cols-2 gap-2">
        <Button variant="secondary">secondary</Button>
        <Button variant="destructive">destructive</Button>
      </div>

      <h3 className="text-lg mt-5 font-bold">Sizes</h3>
      <p className="my-3">
        Buttons are styled with the size prop. If icons are used, the button
        size is inherited and styling is applied to the icon.
      </p>

      <div className=" mt-5">
        <div className="flex gap-2">
          <Button intent="primary" variant="outline">
            Primary
            <ArrowRight />
          </Button>
        </div>

        <div className="flex gap-2 mt-2">
          <Button intent="primary" variant="outline" size="sm">
            Primary
            <CloudHail />
          </Button>
        </div>

        <div className="flex gap-2 mt-2">
          <Button intent="primary" variant="outline" size="lg">
            <Rocket />
            Primary
          </Button>
        </div>
      </div>

      <h3 className="text-lg mt-5 font-bold">Icons</h3>

      <p>Use Lucide Icons directly as children of the button.</p>

      <pre className="bg-stone-100 dark:bg-stone-800 p-4 rounded-lg text-sm overflow-x-auto">
        {`<Button intent="primary" variant="outline" size="lg">
  <Rocket />
  Primary
</Button>`}
      </pre>
    </>
  );
}
