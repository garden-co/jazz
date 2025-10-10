import { CoValueLoadingState, Loaded, CoFeedEntry } from "jazz-tools";
import { useCoState } from "jazz-tools/react";
import { ReactionType, ReactionTypes, Reactions } from "./schema.ts";

const reactionEmojiMap: {
  [reaction in ReactionType]: string;
} = {
  aww: "😍",
  love: "❤️",
  haha: "😂",
  wow: "😮",
  tiny: "🐥",
  chonkers: "🐘",
};

export function ReactionsScreen(props: { id: string }) {
  const reactions = useCoState(Reactions, props.id);

  if (reactions.$jazzState !== CoValueLoadingState.LOADED) return;

  return (
    <>
      <section>
        <h1>Add your reaction</h1>
        <ReactionButtons reactions={reactions} />
      </section>

      <section>
        <h2>Reactions from you and other users</h2>
        <ReactionOverview reactions={reactions} />
      </section>
    </>
  );
}

const ReactionButtons = ({
  reactions,
}: {
  reactions: Loaded<typeof Reactions>;
}) => (
  <div className="reaction-buttons">
    {ReactionTypes.map((reactionType) => (
      <button
        key={reactionType}
        type="button"
        onClick={() => {
          reactions?.$jazz.push(reactionType);
        }}
        title={`React with ${reactionType}`}
        className={reactions?.byMe?.value === reactionType ? "active" : ""}
        data-selected={reactions?.byMe?.value === reactionType}
      >
        {reactionEmojiMap[reactionType]}
      </button>
    ))}
  </div>
);

const ReactionOverview = ({
  reactions,
}: {
  reactions: Loaded<typeof Reactions>;
}) => (
  <>
    {Object.values(reactions.perAccount).map((reaction) => (
      <div key={reaction.by?.$jazz.id} className="reaction-row">
        {reactionEmojiMap[reaction.value as ReactionType]}{" "}
        {getReactorName(reaction)}
      </div>
    ))}
  </>
);

function getReactorName(
  reaction: CoFeedEntry<ReactionType>,
): string | undefined {
  const maybeReactor = reaction.by?.profile;
  if (!maybeReactor || maybeReactor.$jazzState !== CoValueLoadingState.LOADED)
    return;
  return maybeReactor.name;
}
