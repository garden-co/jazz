import { jazzServerAccount } from "@/jazzServerAccount";
import { WaitingRoom } from "@/schema";
import { serverApi } from "@/serverApi";
import { Group } from "jazz-tools";

export async function POST(request: Request) {
  const response = await serverApi.createGame.handle(
    request,
    jazzServerAccount.worker,
    async (_, madeBy) => {
      const waitingRoom = WaitingRoom.create(
        { creator: madeBy },
        Group.create(jazzServerAccount.worker).makePublic(),
      );

      return {
        waitingRoom,
      };
    },
  );

  return response;
}
