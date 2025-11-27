"use client";
import { useAccount } from "jazz-tools/react";
// [!code ++:1]
import Link from "next/link";
import { JazzFestAccount } from "@/app/schema";

export function Festival() {
  const me = useAccount(JazzFestAccount, {
    resolve: { root: { myFestival: { $each: { $onError: "catch" } } } },
  });
  if (!me.$isLoaded) return null;
  return (
    <>
      <ul>
        {me.root.myFestival.map((band) => {
          if (!band.$isLoaded) return null;
          return <li key={band.$jazz.id}>{band.name}</li>;
        })}
      </ul>
      {/* [!code ++:3] */}
      <Link href={`/festival/${me.root.myFestival.$jazz.id}`}>
        Go to my Server-Rendered Festival Page!
      </Link>
    </>
  );
}
