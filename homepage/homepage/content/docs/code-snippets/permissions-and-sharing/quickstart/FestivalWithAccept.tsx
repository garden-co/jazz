"use client";
// @ts-expect-error Duplicate import
import { createInviteLink, useAccount } from "jazz-tools/react";
// [!code ++:2]
import {
  // @ts-expect-error Duplicate import
  createInviteLink,
  useAcceptInvite,
  // @ts-expect-error Duplicate import
  useAccount,
} from "jazz-tools/react";
import { useRouter } from "next/navigation";
import { useState } from "react";
// [!code --:1]
// @ts-expect-error Duplicate import
import { JazzFestAccount } from "@/app/schema";
// [!code ++:2]
// We need to alias the schema because our component is also named Festival
// @ts-expect-error Duplicate import
import { Festival as FestivalSchema, JazzFestAccount } from "@/app/schema";

export function Festival() {
  const [inviteLink, setInviteLink] = useState<string>("");
  const me = useAccount(JazzFestAccount, {
    resolve: { root: { myFestival: { $each: true } } },
  });
  // [!code ++:7]
  const router = useRouter();
  useAcceptInvite({
    invitedObjectSchema: FestivalSchema,
    onAccept: (festivalID: string) => {
      router.push(`/festival/${festivalID}`);
    },
  });
  if (!me.$isLoaded) return null;

  const inviteLinkClickHandler = () => {
    const link = createInviteLink(me.root.myFestival, "writer");
    setInviteLink(link);
  };
  return (
    <>
      <ul>
        {me.root.myFestival.map((band) => (
          <li key={band.$jazz.id}>{band.name}</li>
        ))}
      </ul>
      <input type="text" value={inviteLink} readOnly />
      <button onClick={inviteLinkClickHandler}>Create Invite Link</button>
    </>
  );
}
