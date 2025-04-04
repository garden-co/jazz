import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

/**
 * Given a player selections, returns the winner of the current game.
 */
export function determineWinner(player1Choice: string, player2Choice: string) {
  if (player1Choice === player2Choice) {
    return "It's a tie!";
  } else if (
    (player1Choice === "jazz" && player2Choice === "scissors") ||
    (player1Choice === "paper" && player2Choice === "jazz") ||
    (player1Choice === "scissors" && player2Choice === "paper")
  ) {
    return "Player 1 wins!";
  } else {
    return "Player 2 wins!";
  }
}
