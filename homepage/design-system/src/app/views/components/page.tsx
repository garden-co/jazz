"use client";

import { Button } from "@/components/atoms/Button";
import { Table } from "@/components/molecules/Table";
import {
  Dropdown,
  DropdownButton,
  DropdownItem,
  DropdownMenu,
} from "@/components/organisms/Dropdown";
import { useState } from "react";
import { Style, variants } from "../../../utils/tailwindClassesMap";

export default function DropdownPage() {
  const [selectedVariant, setSelectedVariant] = useState<Style>("default");
  const [selectedVariantIntent, setSelectedVariantIntent] =
    useState<Style>("default");

  return (
    <>
      <h3 className="text-lg mt-5 mb-2 font-bold">DropdownButton Props</h3>
      <p className="my-3">
        DropdownButton can accept design system props like intent, variant, and
        icon.
      </p>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
        <Dropdown>
          <DropdownButton
            className="w-full justify-between"
            as={Button}
            variant="outline"
            intent="primary"
          >
            Primary Button
          </DropdownButton>
          <DropdownMenu>
            <DropdownItem>Option 1</DropdownItem>
            <DropdownItem>Option 2</DropdownItem>
            <DropdownItem>Option 3</DropdownItem>
          </DropdownMenu>
        </Dropdown>

        <Dropdown>
          <DropdownButton
            className="w-full justify-between"
            as={Button}
            variant="inverted"
            intent="success"
            icon="browser"
          >
            Success Inverted
          </DropdownButton>
          <DropdownMenu>
            <DropdownItem>Option 1</DropdownItem>
            <DropdownItem>Option 2</DropdownItem>
            <DropdownItem>Option 3</DropdownItem>
          </DropdownMenu>
        </Dropdown>
      </div>

      <h3 className="text-lg mt-8 mb-2 font-bold">Item Intents</h3>
      <p className="my-3">
        DropdownItems support intents. When selected, they automatically use the
        darker variant.
      </p>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
        <Dropdown>
          <DropdownButton
            className="w-full justify-between"
            as={Button}
            variant="outline"
          >
            {selectedVariantIntent}
          </DropdownButton>
          <DropdownMenu>
            {variants.map((variant) => (
              <DropdownItem
                key={variant}
                intent={variant}
                selected={selectedVariantIntent === variant}
                onClick={() => setSelectedVariantIntent(variant)}
              >
                {variant}
              </DropdownItem>
            ))}
          </DropdownMenu>
        </Dropdown>
      </div>

      <h3 className="text-lg mt-8 mb-2 font-bold">Item Selected Colors</h3>
      <p className="my-3">
        DropdownItems support selectedItemColor. When selected, they
        automatically use the selectedItemColor.
      </p>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
        <Dropdown>
          <DropdownButton
            className="w-full justify-between"
            as={Button}
            variant="outline"
          >
            {selectedVariant}
          </DropdownButton>
          <DropdownMenu>
            {variants.map((variant) => (
              <DropdownItem
                key={variant}
                selected={selectedVariant === variant}
                selectedItemColor={variant}
                onClick={() => setSelectedVariant(variant)}
              >
                {variant}
              </DropdownItem>
            ))}
          </DropdownMenu>
        </Dropdown>
      </div>

      <div className="overflow-auto">
        <h3 className="text-xl mt-5 mb-2 font-bold">Props Table</h3>
        <Table tableData={dropdownPropsTableData} copyable={true} />
      </div>
    </>
  );
}

const dropdownPropsTableData = {
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
      prop: "selected?",
      types: "boolean",
      default: "false",
    },
    {
      prop: "selectedItemColor?",
      types: [
        "primary",
        "tip",
        "info",
        "success",
        "warning",
        "alert",
        "danger",
        "muted",
        "strong",
        "default",
      ],
      default: "primary",
    },
    {
      prop: "href?",
      types: "string",
      default: "undefined",
    },
  ],
};
