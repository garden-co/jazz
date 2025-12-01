"use client";
// [!code --:1]
// @ts-expect-error Duplicate import
import { useAccount } from "jazz-tools/react";
// [!code ++:1]
// @ts-expect-error Duplicate import
import { createInviteLink, useAccount } from "jazz-tools/react";
// [!code ++:1]
import { useState } from "react";
import { JazzFestAccount } from "@/app/schema";

export function Festival() {
  // [!code ++:1]
  const [inviteLink, setInviteLink] = useState<string>("");
  const me = useAccount(JazzFestAccount, {
    resolve: { root: { myFestival: { $each: true } } },
  });
  if (!me.$isLoaded) return null;
  // [!code ++:4]
  const inviteLinkClickHandler = () => {
    const link = createInviteLink(me.root.myFestival, "writer");
    setInviteLink(link);
  };
  return (
    // [!code ++:1]
    <>
      <ul>
        {me.root.myFestival.map((band) => (
          <li key={band.$jazz.id}>{band.name}</li>
        ))}
      </ul>
      {/* [!code ++:5] */}
      <input type="text" value={inviteLink} readOnly />
      <button onClick={inviteLinkClickHandler}>Create Invite Link</button>
    </>
  );
}
