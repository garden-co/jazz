"use client";

import { Group, isJazzRequestError } from "jazz-tools";
import { useAccount, useCoState } from "jazz-tools/react";
import {
  CheckCircle,
  Clock,
  Gamepad2,
  Minus,
  Sparkles,
  Trophy,
  Users,
  XCircle,
} from "lucide-react";
import { useParams } from "next/navigation";
import { useState } from "react";
import { toast } from "sonner";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Game, PlayerState, PlaySelection } from "@/schema";
import { serverApi } from "@/serverApi";

const playIcon = (
  selection: "rock" | "paper" | "scissors" | undefined,
  size: "sm" | "lg" = "sm",
) => {
  const emojiSize = size === "lg" ? "text-4xl" : "text-2xl";

  switch (selection) {
    case "rock":
      return (
        <div className="flex items-center gap-2">
          <span className={`${emojiSize} `} style={{ animationDelay: "0ms" }}>
            🪨
          </span>
          <span className="font-semibold">Rock</span>
        </div>
      );
    case "paper":
      return (
        <div className="flex items-center gap-2">
          <span className={`${emojiSize} `} style={{ animationDelay: "150ms" }}>
            📄
          </span>
          <span className="font-semibold">Paper</span>
        </div>
      );
    case "scissors":
      return (
        <div className="flex items-center gap-2">
          <span className={`${emojiSize} `} style={{ animationDelay: "300ms" }}>
            ✂️
          </span>
          <span className="font-semibold">Scissors</span>
        </div>
      );
    default:
      return (
        <div className="flex items-center gap-2 text-muted-foreground">
          <Clock className={size === "lg" ? "w-8 h-8" : "w-6 h-6"} />
          <span>Waiting for selection</span>
        </div>
      );
  }
};

const getOutcomeIcon = (outcome: string | undefined, player: string) => {
  if (outcome === player) {
    return <Trophy className="w-6 h-6 text-yellow-500" />;
  } else if (outcome === "draw") {
    return <Minus className="w-6 h-6 text-blue-500" />;
  } else {
    return <XCircle className="w-6 h-6 text-red-500" />;
  }
};

export default function RouteComponent() {
  const params = useParams<{ id: string }>();
  const game = useCoState(Game, params.id, {
    resolve: {
      player1State: {
        $onError: null,
      },
      player2State: {
        $onError: null,
      },
      player1: {
        account: true,
        playSelection: {
          $onError: null,
        },
      },
      player2: {
        account: true,
        playSelection: {
          $onError: null,
        },
      },
    },
  });

  const isPlayer1 = game?.player1?.account?.isMe;
  const player = isPlayer1 ? "player1" : "player2";

  if (!game) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100 flex items-center justify-center">
        <div className="animate-pulse">
          <Gamepad2 className="w-12 h-12 text-muted-foreground" />
        </div>
      </div>
    );
  }

  const gameComplete = game.outcome !== undefined;

  const opponent = isPlayer1 ? "player2" : "player1";

  const currentPlayer = game[player];
  const opponentPlayer = game[opponent];

  const currentPlayerState = game[isPlayer1 ? "player1State" : "player2State"];

  const opponentSelection = opponentPlayer?.playSelection;
  const opponentHasSelected = Boolean(opponentPlayer._refs.playSelection);

  const handleSelection = (selection: "rock" | "paper" | "scissors") => {
    if (!currentPlayerState) return;
    if (currentPlayerState.submitted) return;

    currentPlayerState.currentSelection = selection;
  };

  const handleSubmit = async (
    playSelection: "rock" | "paper" | "scissors" | undefined,
  ) => {
    if (!playSelection) return;
    if (!currentPlayerState) return;

    currentPlayerState.submitted = true;

    try {
      await serverApi.play.send({
        game,
        selection: playSelection,
      });
    } catch (error) {
      currentPlayerState.submitted = false;
      if (isJazzRequestError(error)) {
        toast.error(error.message);
      } else {
        console.error(error);
        toast.error("An unexpected error occurred");
      }
    }
  };

  const handleNewGame = async () => {
    if (!currentPlayerState) return;

    currentPlayerState.resetRequested = true;

    try {
      await serverApi.newGame.send({
        game,
      });
    } catch (error) {
      currentPlayerState.resetRequested = false;

      if (isJazzRequestError(error)) {
        toast.error(error.message);
      } else {
        console.error(error);
        toast.error("An unexpected error occurred");
      }
    }
  };

  const playSelection = currentPlayerState?.currentSelection;

  const submitDisabled =
    !currentPlayerState?.currentSelection || currentPlayerState?.submitted;

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100 py-8 px-4">
      <div className="max-w-4xl mx-auto">
        {/* Header */}
        <div className="text-center mb-8">
          <div className="flex items-center justify-center gap-2 mb-4">
            <Gamepad2 className="w-8 h-8 text-primary" />
            <h1 className="text-4xl font-bold bg-gradient-to-r from-primary to-purple-600 bg-clip-text text-transparent">
              Rock, Paper, Scissors
            </h1>
            <Sparkles className="w-6 h-6 text-yellow-500" />
          </div>

          {/* Player Info */}
          <div className="flex items-center justify-center gap-4 mb-6">
            <Badge variant="secondary" className="px-4 py-2">
              <Users className="w-4 h-4 mr-2" />
              {isPlayer1 ? "Player 1" : "Player 2"}
            </Badge>
          </div>

          {/* Score Board */}
          <Card className="inline-block bg-white/80 backdrop-blur-sm border-0 shadow-lg">
            <CardContent className="p-4">
              <div className="flex items-center gap-6 text-2xl font-bold">
                <div className="flex items-center gap-2">
                  <span className="text-blue-600">Player 1</span>
                  <span className="text-3xl">{game?.player1Score ?? 0}</span>
                </div>
                <div className="text-muted-foreground">-</div>
                <div className="flex items-center gap-2">
                  <span className="text-3xl">{game?.player2Score ?? 0}</span>
                  <span className="text-purple-600">Player 2</span>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Game Outcome */}
        {gameComplete && (
          <Card className="mb-8 bg-gradient-to-r from-yellow-50 to-orange-50 border-yellow-200">
            <CardContent className="p-6 text-center">
              <div className="flex items-center justify-center gap-3 mb-4">
                {getOutcomeIcon(game.outcome, player)}
                <h2 className="text-2xl font-bold">
                  {game?.outcome === player
                    ? "🎉 You Win! 🎉"
                    : game?.outcome === "draw"
                      ? "🤝 It's a Draw! 🤝"
                      : "😔 You Lose! 😔"}
                </h2>
              </div>
              <Button
                disabled={currentPlayerState?.resetRequested}
                onClick={handleNewGame}
                className="bg-gradient-to-r from-primary to-purple-600 hover:from-primary/90 hover:to-purple-600/90 text-white"
                size="lg"
              >
                <Gamepad2 className="w-5 h-5 mr-2" />
                Start New Game
              </Button>
            </CardContent>
          </Card>
        )}

        {/* Game Board */}
        <div className="grid md:grid-cols-2 gap-8">
          {/* Your Selection */}
          <Card className="bg-white/80 backdrop-blur-sm border-0 shadow-lg">
            <CardHeader className="text-center pb-4">
              <CardTitle className="text-xl font-semibold text-blue-600">
                Your Selection
              </CardTitle>
            </CardHeader>
            <CardContent className="text-center">
              <div className="mb-6">{playIcon(playSelection, "lg")}</div>

              {!gameComplete && (
                <>
                  {/* Choice Buttons */}
                  <div className="grid grid-cols-3 gap-4 mb-6">
                    <Button
                      variant={playSelection === "rock" ? "default" : "outline"}
                      size="lg"
                      className={`h-20 transition-all duration-200 ${
                        playSelection === "rock"
                          ? "bg-gradient-to-br from-gray-400 to-gray-600 text-white shadow-lg scale-105"
                          : "hover:scale-105"
                      } text-3xl`}
                      onClick={() => handleSelection("rock")}
                      aria-label="Select Rock"
                      aria-selected={playSelection === "rock"}
                    >
                      🪨
                    </Button>
                    <Button
                      variant={
                        playSelection === "paper" ? "default" : "outline"
                      }
                      size="lg"
                      className={`h-20 transition-all duration-200 ${
                        playSelection === "paper"
                          ? "bg-gradient-to-br from-blue-400 to-blue-600 text-white shadow-lg scale-105"
                          : "hover:scale-105"
                      } text-3xl`}
                      onClick={() => handleSelection("paper")}
                      aria-label="Select Paper"
                      aria-selected={playSelection === "paper"}
                    >
                      📄
                    </Button>
                    <Button
                      variant={
                        playSelection === "scissors" ? "default" : "outline"
                      }
                      size="lg"
                      className={`h-20 transition-all duration-200 ${
                        playSelection === "scissors"
                          ? "bg-gradient-to-br from-red-400 to-red-600 text-white shadow-lg scale-105"
                          : "hover:scale-105"
                      } text-3xl`}
                      onClick={() => handleSelection("scissors")}
                      aria-label="Select Scissors"
                      aria-selected={playSelection === "scissors"}
                    >
                      ✂️
                    </Button>
                  </div>

                  {/* Submit Button */}
                  <Button
                    disabled={submitDisabled}
                    onClick={() => handleSubmit(playSelection)}
                    className="w-full bg-gradient-to-r from-green-500 to-emerald-600 hover:from-green-600 hover:to-emerald-700 text-white font-semibold py-3 text-lg"
                    size="lg"
                  >
                    {currentPlayerState?.submitted ? (
                      <>
                        <CheckCircle className="w-5 h-5 mr-2" />
                        Selection Made!
                      </>
                    ) : (
                      <>
                        <Gamepad2 className="w-5 h-5 mr-2" />
                        Make Your Move!
                      </>
                    )}
                  </Button>
                </>
              )}
            </CardContent>
          </Card>

          {/* Opponent Selection */}
          <Card className="bg-white/80 backdrop-blur-sm border-0 shadow-lg">
            <CardHeader className="text-center pb-4">
              <CardTitle className="text-xl font-semibold text-purple-600">
                Opponent's Selection
              </CardTitle>
            </CardHeader>
            <CardContent className="text-center">
              {opponentSelection && (
                <div className="mb-6">
                  {playIcon(opponentSelection?.value, "lg")}
                </div>
              )}

              {!opponentSelection && !gameComplete ? (
                opponentHasSelected ? (
                  <div className="text-muted-foreground">
                    <Clock className="w-8 h-8 mx-auto mb-2 animate-pulse" />
                    <p>The opponent has made their move</p>
                  </div>
                ) : (
                  <div className="text-muted-foreground">
                    <Clock className="w-8 h-8 mx-auto mb-2 animate-pulse" />
                    <p>Waiting for opponent...</p>
                  </div>
                )
              ) : null}
            </CardContent>
          </Card>
        </div>

        {/* Game Status */}
        {!gameComplete && (
          <Card className="mt-8 bg-white/60 backdrop-blur-sm border-0">
            <CardContent className="p-4 text-center">
              <div className="flex items-center justify-center gap-2 text-muted-foreground">
                <Clock className="w-4 h-4" />
                <span className="text-sm">
                  {currentPlayer?.playSelection && !opponentSelection
                    ? "Waiting for opponent to make their move..."
                    : "Make your selection to start the game"}
                </span>
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
}
