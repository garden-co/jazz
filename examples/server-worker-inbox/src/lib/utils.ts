import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

/**
 * Given a player selections, returns the winner of the current game.
 */
export function determineWinner(
  player1Choice: "rock" | "paper" | "scissors",
  player2Choice: "rock" | "paper" | "scissors",
) {
  if (player1Choice === player2Choice) {
    return "draw";
  } else if (
    (player1Choice === "rock" && player2Choice === "scissors") ||
    (player1Choice === "paper" && player2Choice === "rock") ||
    (player1Choice === "scissors" && player2Choice === "paper")
  ) {
    return "player1";
  } else {
    return "player2";
  }
}
