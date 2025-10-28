"use client";
import { Button } from "@/components/atoms/Button";
import { Icon } from "@/components/atoms/Icon";
import { Table } from "@/components/molecules/Table";
import {
  Dropdown,
  DropdownButton,
  DropdownItem,
  DropdownMenu,
} from "@/components/organisms/Dropdown";
import { useState } from "react";
import { Style } from "../../../utils/tailwindClassesMap";
export default function ButtonsPage() {
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
  const [selectedVariant, setSelectedVariant] = useState<Style>("default");
  return (
    <>
      <h3 className="text-lg mt-5 mb-2 font-bold">Variants</h3>

      <p className="my-3">Buttons are styled with the variant prop.</p>

      <div className="grid grid-cols-2 gap-2">
        <Button variant="default">default</Button>
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
        <div className="max-w-xs ml-3">
          <Dropdown>
            <DropdownButton
              className="w-full justify-between"
              as={Button}
              intent="default"
              variant="inverted"
            >
              {selectedVariant}
              <Icon name="chevronDown" size="sm" />
            </DropdownButton>
            <DropdownMenu>
              {variants.map((variant) => (
                <DropdownItem
                  key={variant}
                  onClick={() => setSelectedVariant(variant)}
                >
                  {variant}
                </DropdownItem>
              ))}
            </DropdownMenu>
          </Dropdown>
        </div>
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

      <h3 className="text-lg font-bold mt-5">Icons</h3>

      <p className="my-3">Buttons can also contain an icon and text.</p>

      <div className="grid grid-cols-2 gap-2">
        <Button
          icon="delete"
          intent="danger"
          variant="link"
          iconPosition="right"
          className="col-span-2 md:col-span-1"
        >
          text danger with icon
        </Button>
        <Button
          icon="info"
          iconPosition="left"
          intent="info"
          variant="outline"
          className="col-span-2 md:col-span-1"
        >
          outline info with icon
        </Button>
        <p className="col-span-2 my-2">
          Or just use the icon prop with any of the button variants, style
          variants and colors.
        </p>
        <Button icon="newsletter" intent="tip" variant="inverted" />
        <Button icon="check" intent="success" />
      </div>
      <div className="overflow-auto">
        <h3 className="text-xl mt-5 mb-2 font-bold">Props Table</h3>
        <Table tableData={buttonPropsTableData} copyable={true} />
      </div>
    </>
  );
}

const buttonPropsTableData = {
  headers: ["prop", "types", "default"],
  data: [
    {
      prop: "intent?",
      types: [
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
      ],
      default: "default",
    },
    {
      prop: "variant?",
      types: [
        "default",
        "outline",
        "inverted",
        "ghost",
        "link",
        "secondary",
        "destructive",
      ],
      default: "default",
    },
    {
      prop: "icon?",
      types: "Lucide icon name",
      default: "undefined",
    },
    {
      prop: "iconPosition?",
      types: ["left", "right"],
      default: "left",
    },
    {
      prop: "loading?",
      types: "boolean",
      default: "false",
    },
    {
      prop: "loadingText?",
      types: "string",
      default: "Loading...",
    },
    {
      prop: "disabled?",
      types: "boolean",
      default: "false",
    },
    {
      prop: "href?",
      types: "string",
      default: "undefined",
    },
    {
      prop: "newTab?",
      types: "boolean",
      default: "false",
    },
    {
      prop: "size?",
      types: ["sm", "md", "lg"],
      default: "md",
    },
    {
      prop: "className?",
      types: "string",
      default: "undefined",
    },
    {
      prop: "children?",
      types: "React.ReactNode",
      default: "undefined",
    },
  ],
};
