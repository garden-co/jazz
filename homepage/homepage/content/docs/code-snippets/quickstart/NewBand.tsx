"use client";
import { useAccount } from "jazz-tools/react";
import { JazzFestAccount } from "@/app/schema";
import { useState } from "react";

export function NewBand() {
  const me = useAccount(JazzFestAccount, {
    resolve: { root: { myFestival: true } },
  });
  const [name, setName] = useState("");

  const handleSave = () => {
    if (!me.$isLoaded) return;
    me.root.myFestival.$jazz.push({ name });
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
