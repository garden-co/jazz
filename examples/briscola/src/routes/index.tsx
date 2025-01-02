import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import { useAccount, useInboxSender } from "@/jazz";
import { StartGameRequest } from "@/schema";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { Account, Group } from "jazz-tools";

const workerId = import.meta.env.VITE_JAZZ_WORKER_ACCOUNT;

export const Route = createFileRoute("/")({
  component: HomeComponent,
});

function HomeComponent() {
  const createGame = useInboxSender(workerId);
  const { me } = useAccount();
  const navigate = useNavigate({ from: "/" });

  const onNewGameClick = async () => {
    const account = await Account.load(workerId, me, {});

    if (!account) {
      return;
    }

    const waitingRoomId = await createGame(
      StartGameRequest.create({}, { owner: Group.create({ owner: me }) }),
    );

    if (!waitingRoomId) {
      return;
    }

    navigate({ to: `/waiting-room/${waitingRoomId}` });
  };

  return (
    <div className="h-screen flex flex-col w-full place-items-center justify-center p-2">
      <Card className="w-[500px]">
        <CardHeader>
          <CardTitle>Welcome to Jazz Briscola</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <div className="flex items-center space-x-4">
            <div className="w-1/2 flex flex-col p-4">
              <Button onClick={onNewGameClick}>New Game</Button>
            </div>
            <Separator orientation="vertical" className="h-40" />
            <div className="w-1/2 flex flex-col space-y-4 p-4">
              <div className="flex flex-col space-y-2">
                <Label htmlFor="picture">Game ID</Label>
                <Input id="picture" placeholder="co_XXXXXXXXXXX" />
              </div>
              <Button>Join</Button>
            </div>
          </div>
          <Separator />
          <div className="p-4">
            <Button variant="link">How to play?</Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
