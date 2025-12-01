"use client";
import {
  createInviteLink,
  useAcceptInvite,
  useAccount,
  // [!code ++:1]
  useCoState,
} from "jazz-tools/react";
import { useRouter } from "next/navigation";
import { useState } from "react";
// We need to alias the schema because our component is also named Festival
import { Festival as FestivalSchema, JazzFestAccount } from "@/app/schema";

// [!code ++:1]
export function Festival({ id }: { id?: string }) {
  const [inviteLink, setInviteLink] = useState<string>("");
  const me = useAccount(JazzFestAccount, {
    resolve: { root: { myFestival: true } },
  });
  const router = useRouter();
  useAcceptInvite({
    invitedObjectSchema: FestivalSchema,
    onAccept: (festivalID: string) => {
      router.push(`/festival/${festivalID}`);
    },
  });
  // [!code ++:2]
  const festivalId =
    id ?? (me.$isLoaded ? me.root.myFestival.$jazz.id : undefined);
  const festival = useCoState(FestivalSchema, festivalId);
  // [!code --:1]
  if (!me.$isLoaded) return null;
  // [!code ++:1]
  if (!festival.$isLoaded) return null;
  const inviteLinkClickHandler = () => {
    // [!code --:1]
    // @ts-expect-error duplicate
    const link = createInviteLink(me.root.myFestival, "writer");
    // [!code ++:1]
    // @ts-expect-error duplicate
    const link = createInviteLink(festival, "writer");
    setInviteLink(link);
  };
  return (
    <>
      <ul>
        {/* [!code --:3] */}
        {me.root.myFestival.map((band) => {
          return band.$isLoaded && <li key={band.$jazz.id}>{band.name}</li>;
        })}
        {/* [!code ++:3] */}
        {festival.map(
          (band) => band.$isLoaded && <li key={band.$jazz.id}>{band.name}</li>,
        )}
      </ul>
      {me.canAdmin(festival) && (
        <>
          <input type="text" value={inviteLink} readOnly />
          <button type="button" onClick={inviteLinkClickHandler}>
            Create Invite Link
          </button>
        </>
      )}
    </>
  );
}
