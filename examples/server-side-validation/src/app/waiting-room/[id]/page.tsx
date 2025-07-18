"use client";

import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { WaitingRoom } from "@/schema";
import { serverApi } from "@/serverApi";
import { JazzRequestError, co } from "jazz-tools";
import { useCoState } from "jazz-tools/react-core";
import { ClipboardCopyIcon, Loader2Icon } from "lucide-react";
import { useParams, useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import { toast } from "sonner";

function useWindowLocation() {
  const [location, setLocation] = useState<string>("");

  useEffect(() => {
    setLocation(window.location.href);
  }, []);

  return location;
}

async function askToJoinGame(
  waitingRoom: co.loaded<typeof WaitingRoom, { creator: true }>,
) {
  if (waitingRoom.creator.isMe) {
    return null;
  }

  try {
    await serverApi.joinGame.send({
      waitingRoom,
    });
  } catch (error) {
    if (error instanceof JazzRequestError) {
      toast.error(error.message);
    } else {
      toast.error("An unexpected error occurred");
    }
  }
}

export default function RouteComponent() {
  const params = useParams<{ id: string }>();
  const waitingRoom = useCoState(WaitingRoom, params.id, {
    resolve: {
      creator: true,
      game: true,
    },
  });
  const [copied, setCopied] = useState(false);
  const router = useRouter();
  const location = useWindowLocation();

  useEffect(() => {
    if (!waitingRoom) {
      return;
    }

    askToJoinGame(waitingRoom);
  }, [waitingRoom?.id]);

  useEffect(() => {
    if (!waitingRoom?.game?.id) {
      return;
    }

    router.push(`/game/${waitingRoom.game.id}`);
  }, [waitingRoom?.game?.id]);

  const onCopyClick = () => {
    navigator.clipboard.writeText(window.location.toString());
    setCopied(true);
    toast.success("Link copied to clipboard!");
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-indigo-900 via-purple-900 to-pink-900 flex flex-col items-center justify-center p-4 relative overflow-hidden">
      {/* Background decoration */}
      <div className="absolute inset-0 overflow-hidden">
        <div className="absolute -top-40 -right-40 w-80 h-80 bg-white/5 rounded-full blur-3xl"></div>
        <div className="absolute -bottom-40 -left-40 w-80 h-80 bg-white/5 rounded-full blur-3xl"></div>
      </div>

      {/* Main content */}
      <div className="relative z-10 text-center space-y-8 max-w-2xl mx-auto">
        {/* Game title and emojis */}
        <div className="space-y-4">
          <div className="flex justify-center items-center space-x-4 text-6xl mb-6">
            <span className="animate-bounce" style={{ animationDelay: "0ms" }}>
              🪨
            </span>
            <span
              className="animate-bounce"
              style={{ animationDelay: "150ms" }}
            >
              📄
            </span>
            <span
              className="animate-bounce"
              style={{ animationDelay: "300ms" }}
            >
              ✂️
            </span>
          </div>
          <h1 className="text-5xl font-bold text-white mb-2">Waiting Room</h1>
          <p className="text-xl text-gray-300 max-w-md mx-auto">
            Share this link with your friend to join the game
          </p>
        </div>

        {/* Waiting room card */}
        <Card className="w-full max-w-md mx-auto bg-white/10 backdrop-blur-lg border-white/20 shadow-2xl">
          <CardHeader className="text-center pb-4">
            <CardTitle className="text-2xl font-bold text-white flex items-center justify-center">
              <Loader2Icon className="animate-spin inline h-8 w-8 mr-3" />
              Waiting for opponent
            </CardTitle>
            <CardDescription className="text-gray-300 text-sm mt-2">
              The game will automatically start once they join
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex">
              <Input
                className="w-full border-white/20 bg-white/5 text-white placeholder:text-gray-400 rounded-e-none focus:border-white/40 focus:ring-white/20"
                readOnly
                value={location}
              />
              <Button
                onClick={onCopyClick}
                className="rounded-s-none w-25 bg-gradient-to-r from-purple-500 to-pink-500 hover:from-purple-600 hover:to-pink-600 border-0 shadow-lg hover:shadow-xl transition-all duration-300 transform hover:scale-105"
              >
                {copied ? (
                  "Copied!"
                ) : (
                  <>
                    <ClipboardCopyIcon className="w-5 h-5" />
                    Copy
                  </>
                )}
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Footer */}
        <div className="text-center text-gray-400 text-sm mt-12">
          <p>Built with Jazz Framework</p>
        </div>
      </div>
    </div>
  );
}
