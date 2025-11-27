"use client";
import {
  createInviteLink,
  useAcceptInvite,
  useAccount,
  useAgent,
  useCoState,
} from "jazz-tools/react";
import { useRouter } from "next/navigation";
import { useState } from "react";
import { Festival as FestivalSchema, JazzFestAccount } from "@/app/schema";

export function Festival({ id }: { id?: string }) {
  const [inviteLink, setInviteLink] = useState<string>("");
  // [!code ++:1]
  const agent = useAgent();
  const me = useAccount(JazzFestAccount, {
    resolve: { root: { myFestival: true } },
  });
  const router = useRouter();
  // [!code ++:2]
  const isGuest = agent.$type$ !== "Account";
  if (!isGuest) {
    useAcceptInvite({
      invitedObjectSchema: FestivalSchema,
      onAccept: (festivalID: string) => {
        router.push(`/festival/${festivalID}`);
      },
    });
    // [!code ++:1]
  }

  const festivalId =
    id ?? (me.$isLoaded ? me.root.myFestival.$jazz.id : undefined);
  const festival = useCoState(FestivalSchema, festivalId);
  if (!festival.$isLoaded) return null;
  const inviteLinkClickHandler = () => {
    const link = createInviteLink(festival, "writer");
    setInviteLink(link);
  };
  return (
    <>
      <ul>
        {festival.map(
          (band) => band.$isLoaded && <li key={band.$jazz.id}>{band.name}</li>,
        )}
      </ul>
      <input type="text" value={inviteLink} readOnly />
      <button type="button" onClick={inviteLinkClickHandler}>
        Create Invite Link
      </button>
    </>
  );
}
