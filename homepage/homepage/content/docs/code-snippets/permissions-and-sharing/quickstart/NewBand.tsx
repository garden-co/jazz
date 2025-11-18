"use client";
// [!code --:2]
// @ts-expect-error Duplicate declaration
import { useAccount } from "jazz-tools/react";
// @ts-expect-error Duplicate declaration
import { JazzFestAccount } from "@/app/schema";
// [!code ++:3]
// @ts-expect-error Duplicate declaration
import { useAccount, useCoState } from "jazz-tools/react";
import { useState } from "react";
// @ts-expect-error Duplicate declaration
import { JazzFestAccount, Festival } from "@/app/schema";

// [!code ++:1]
export function NewBand({ id }: { id?: string }) {
  const me = useAccount(JazzFestAccount, {
    resolve: { root: { myFestival: true } },
  });
  const [name, setName] = useState("");

  // [!code ++:2]
  const festivalId =
    id ??
    (me.$isLoaded && me.root.$isLoaded
      ? me.root.myFestival.$jazz.id
      : undefined);
  const festival = useCoState(Festival, festivalId);

  const handleSave = () => {
    // [!code --:2]
    if (!me.$isLoaded) return;
    me.root.myFestival.$jazz.push({ name });
    // [!code ++:2]
    if (!festival.$isLoaded) return;
    festival.$jazz.push({ name });
    setName("");
  };

  return (
    <div>
      <input
        type="text"
        value={name}
        placeholder="Band name"
        onChange={(e) => setName(e.target.value)}
      />
      <button type="button" onClick={handleSave}>
        Add
      </button>
    </div>
  );
}
