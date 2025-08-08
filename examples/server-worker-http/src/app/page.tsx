"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { serverApi } from "@/serverApi";

export default function HomeComponent() {
  const [isLoading, setIsLoading] = useState(false);
  const router = useRouter();

  const onNewGameClick = async () => {
    setIsLoading(true);

    const { waitingRoom } = await serverApi.createGame.send({});

    router.push(`/waiting-room/${waitingRoom.id}`);
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
          <h1 className="text-5xl font-bold text-white mb-2">
            Rock, Paper, Scissors
          </h1>
          <p className="text-xl text-gray-300 max-w-md mx-auto">
            Challenge your friends in this classic multiplayer game powered by
            Jazz
          </p>
        </div>

        {/* Game card */}
        <Card className="w-full max-w-md mx-auto bg-white/10 backdrop-blur-lg border-white/20 shadow-2xl">
          <CardHeader className="text-center pb-4">
            <CardTitle className="text-2xl font-bold text-white">
              Ready to Play?
            </CardTitle>
            <p className="text-gray-300 text-sm mt-2">
              Create a new game and invite your friends
            </p>
          </CardHeader>
          <CardContent className="space-y-4">
            <Button
              onClick={onNewGameClick}
              disabled={isLoading}
              className="w-full h-12 text-lg font-semibold bg-gradient-to-r from-purple-500 to-pink-500 hover:from-purple-600 hover:to-pink-600 border-0 shadow-lg hover:shadow-xl transition-all duration-300 transform hover:scale-105"
            >
              {isLoading ? (
                <div className="flex items-center space-x-2">
                  <div className="w-5 h-5 border-2 border-white/30 border-t-white rounded-full animate-spin"></div>
                  <span>Creating game...</span>
                </div>
              ) : (
                <div className="flex items-center space-x-2">
                  <span>🎮</span>
                  <span>Start New Game</span>
                </div>
              )}
            </Button>
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
