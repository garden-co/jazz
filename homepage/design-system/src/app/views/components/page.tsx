"use client";

import { Switch } from "@/components/atoms/Switch";
import { useState } from "react";

export default function Components() {
  const [checked, setChecked] = useState({
    md: true,
    sm: true,
  });

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
      </div>
    </div>
  );
}
