import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import { WORKER_ID } from "@/constants";
import { useAccount, useInboxSender } from "@/jazz";
import { CreateGameRequest } from "@/schema";
import { Link, createFileRoute, useNavigate } from "@tanstack/react-router";
import { Group } from "jazz-tools";
import { ExternalLinkIcon } from "lucide-react";
import { useState } from "react";

export const Route = createFileRoute("/")({
  component: HomeComponent,
});

function HomeComponent() {
  const createGame = useInboxSender(WORKER_ID);
  const { me } = useAccount();
  const navigate = useNavigate({ from: "/" });
  const [isLoading, setIsLoading] = useState(false);

  const onNewGameClick = async () => {
    setIsLoading(true);

    const waitingRoomId = await createGame(
      CreateGameRequest.create(
        {
          type: "createGame",
        },
        { owner: Group.create({ owner: me }) },
      ),
    );

    if (!waitingRoomId) {
      setIsLoading(false);
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
          <div className="flex items-center p-4">
            <Button
              onClick={onNewGameClick}
              loading={isLoading}
              loadingText="Creating game..."
              className="w-full"
            >
              New Game
            </Button>
          </div>
          <Separator />
          <div className="p-4 flex items-center justify-between">
            <Button variant="link" asChild>
              <Link to="/how-to-play">How to play?</Link>
            </Button>
            <Button variant="link" asChild className="text-muted-foreground">
              <Link
                href="https://en.wikipedia.org/wiki/Briscola"
                target="_blank"
                rel="noreferrer"
              >
                Briscola on Wikipedia
                <ExternalLinkIcon className="w-4 h-4 ml-1" />
              </Link>
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
