"use client";
import { useAccount } from "jazz-tools/react";
import { JazzFestAccount } from "@/app/schema";

export function Festival() {
  const me = useAccount(JazzFestAccount, {
    resolve: {
      root: {
        myFestival: {
          $each: true
        }
      }
    },
  });
  if (!me.$isLoaded) return null;
  return (
    <ul>
      {me.root.myFestival.map(
        (band) => band && <li key={band.$jazz.id}>{band.name}</li>,
      )}
    </ul>
  );
}
