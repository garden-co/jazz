"use client";

import { Button } from "@/components/atoms/Button";
import { Switch } from "@/components/atoms/Switch";
import { DropdownButton } from "@/components/organisms/Dropdown";
import {
  Dropdown,
  DropdownItem,
  DropdownMenu,
} from "@/components/organisms/Dropdown";
import { useState } from "react";
import { variants } from "../../../utils/tailwindClassesMap";

export default function Components() {
  const [checked, setChecked] = useState({
    md: true,
    sm: true,
  });

  const [selectedVariant, setSelectedVariant] = useState("default");

  return (
    <div className="p-3">
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
        <div className="max-w-xs ml-3">
          <Dropdown>
            <DropdownButton
              className="w-full justify-between"
              as={Button}
              intent="default"
              variant="inverted"
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
              <DropdownItem intent="danger">{selectedVariant}</DropdownItem>
            </DropdownMenu>
          </Dropdown>
        </div>
      </div>
    </div>
  );
}
