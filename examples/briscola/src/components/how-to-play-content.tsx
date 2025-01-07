export function HowToPlayContent() {
  return (
    <div className="">
      <h3 className="font-semibold text-lg">Objective:</h3>
      <p className="mt-2">
        The goal is to score the most points by winning tricks containing
        high-value cards. The game is played until all card are played.
      </p>

      <h3 className="font-semibold text-lg mt-6">The deck</h3>
      <p className="mt-2">
        A deck with 40 cards is used, split into four suits:
        <ul className="list-disc list-inside">
          <li className="list-item">Coins (Denari)</li>
          <li className="list-item">Cups (Coppe)</li>
          <li className="list-item">Swords (Spade)</li>
          <li className="list-item">Clubs (Bastoni)</li>
        </ul>
      </p>
      <p className="mt-2">Each suit has cards numbered from 1 to 10.</p>

      <h3 className="font-semibold text-lg mt-6">Card values</h3>
      <p className="mt-2">
        Each card has a point value:
        <ul className="list-disc list-inside">
          <li className="list-item">Ace (1): 11 points</li>
          <li className="list-item">Three (3): 10 points</li>
          <li className="list-item">Eight (8): 2 points</li>
          <li className="list-item">Nine (9): 3 points</li>
          <li className="list-item">Ten (10): 4 points</li>
          <li className="list-item">All others (2, 4-7): 0 points</li>
        </ul>
      </p>
      <p className="mt-2">
        There are <span className="font-semibold">120 total points</span> in the
        deck.
      </p>

      <h3 className="font-semibold text-lg mt-6">Gameplay</h3>
      <p className="mt-2">
        <ol className="list-inside list-decimal">
          <li>
            <span className="font-semibold">Starting the game:</span>
            <ul className="list-inside list-disc">
              <li className="list-item ml-4">
                3 cards are dealt to each player.
              </li>
              <li className="list-item ml-4">
                1 card is placed face-up on the table, on the bottom of the draw
                pile, indicating the trump suit. (Briscola)
              </li>
              <li className="list-item ml-4">
                One player is randomly chosen to start the game.
              </li>
            </ul>
          </li>

          <li>
            <span className="font-semibold">Starting a trick:</span>
            <ul className="list-inside list-disc">
              <li className="list-item ml-4">
                Players play one card each in turn, trying to win the trick.
              </li>
            </ul>
          </li>

          <li>
            <span className="font-semibold">Winning a trick:</span>
            <ul className="list-inside list-disc">
              <li className="list-item ml-4">
                The highest card of the trump suit wins the trick.
              </li>
              <li className="list-item ml-4">
                If no trump cards are played, the highest card of the leading
                suit wins.
              </li>
              <li className="list-item ml-4">
                The leading suit is the suit of the first card played in the
                current trick.
              </li>
              <li className="list-item ml-4">
                The winner collects the cards, which are placed face-up in their
                scoring pile.
              </li>
            </ul>
          </li>

          <li>
            <span className="font-semibold">Drawing cards:</span>
            <ul className="list-inside list-disc">
              <li className="list-item ml-4">
                After each trick, a new card is dealt to each player (starting
                with the trick winner).
              </li>
            </ul>
          </li>

          <li>
            <span className="font-semibold">Continuing play:</span>
            <ul className="list-inside list-disc">
              <li className="list-item ml-4">
                The winner of the previous trick leads the next round.
              </li>
            </ul>
          </li>

          <li>
            <span className="font-semibold">End of the game:</span>
            <ul className="list-inside list-disc">
              <li className="list-item ml-4">
                Play continues until all cards are played.
              </li>
            </ul>
          </li>
        </ol>
      </p>
    </div>
  );
}
