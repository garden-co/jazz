import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { WORKER_ID } from "@/constants";
import { CreateGameRequest } from "@/schema";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { experimental_useInboxSender as useInboxSender } from "jazz-react";
import { useState } from "react";

export const Route = createFileRoute("/")({
  component: HomeComponent,
});

function HomeComponent() {
  const createGame = useInboxSender(WORKER_ID);
  const navigate = useNavigate({ from: "/" });
  const [isLoading, setIsLoading] = useState(false);

  const onNewGameClick = async () => {
    setIsLoading(true);

    const waitingRoomId = await createGame(
      CreateGameRequest.create({
        type: "createGame",
      }),
    );
    if (!waitingRoomId) {
      setIsLoading(false);
      return;
    }
    navigate({ to: `/waiting-room/$waitingRoomId`, params: { waitingRoomId } });
  };

  return (
    <div className="h-screen flex flex-col w-full place-items-center justify-center p-2">
      <Card className="w-[500px]">
        <CardHeader>
          <CardTitle>Welcome to Jazz, Paper, Scissors!</CardTitle>
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
        </CardContent>
      </Card>
    </div>
  );
}
