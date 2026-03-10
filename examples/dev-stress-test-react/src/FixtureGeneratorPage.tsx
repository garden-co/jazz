import { useCallback, useState } from "react";
import { useDb, useSession } from "jazz-tools/react";
import { app } from "../schema/app.js";

const FIXTURE_BATCH_SIZE = 1_000;
const FIXTURE_TOTAL_ROWS = 15_000;

const RANDOM_WORDS = [
  "amber",
  "atlas",
  "breeze",
  "comet",
  "ember",
  "fable",
  "fjord",
  "glow",
  "haze",
  "ink",
  "jewel",
  "kale",
  "lumen",
  "moss",
  "nova",
  "oasis",
  "pulse",
  "quark",
  "raven",
  "spark",
  "thrift",
  "umbra",
  "verge",
  "wisp",
  "xenon",
  "zephyr",
] as const;
function getRandomWord(): string {
  return RANDOM_WORDS[Math.floor(Math.random() * RANDOM_WORDS.length)];
}

function createRandomText(seed: number): string {
  const [first, second, third] = [getRandomWord(), getRandomWord(), getRandomWord()];
  return `${first}-${second}-${third}-${seed + 1}`;
}

function waitNextTick(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

type FixtureGeneratorPageProps = {
  onNavigateTodos: () => void;
};

export function FixtureGeneratorPage({ onNavigateTodos }: FixtureGeneratorPageProps) {
  const db = useDb();
  const session = useSession();
  const sessionUserId = session?.user_id ?? null;
  const [isGeneratingFixtures, setIsGeneratingFixtures] = useState(false);
  const [fixtureStatus, setFixtureStatus] = useState("");

  const generateFixtures = useCallback(async () => {
    if (!sessionUserId || isGeneratingFixtures) return;
    setIsGeneratingFixtures(true);
    setFixtureStatus("Generating projects...");

    try {
      const projectIds: string[] = [];

      for (let batchStart = 0; batchStart < FIXTURE_TOTAL_ROWS; batchStart += FIXTURE_BATCH_SIZE) {
        const batchEnd = Math.min(batchStart + FIXTURE_BATCH_SIZE, FIXTURE_TOTAL_ROWS);
        for (let i = batchStart; i < batchEnd; i++) {
          const { id } = db.insert(app.projects, {
            name: `Project ${createRandomText(i)}`,
            owner_id: sessionUserId,
          });
          projectIds.push(id);
        }
        setFixtureStatus(`Projects: ${batchEnd}/${FIXTURE_TOTAL_ROWS}`);
        await waitNextTick();
      }

      setFixtureStatus("Generating todos...");

      for (let batchStart = 0; batchStart < FIXTURE_TOTAL_ROWS; batchStart += FIXTURE_BATCH_SIZE) {
        const batchEnd = Math.min(batchStart + FIXTURE_BATCH_SIZE, FIXTURE_TOTAL_ROWS);
        for (let i = batchStart; i < batchEnd; i++) {
          const projectIndex = i % projectIds.length;
          db.insert(app.todos, {
            title: `Todo ${createRandomText(i)}`,
            done: i % 2 === 0,
            description: `Generated ${createRandomText(i + FIXTURE_TOTAL_ROWS)}`,
            owner_id: sessionUserId,
            project: projectIds[projectIndex],
          });
        }
        setFixtureStatus(`Todos: ${batchEnd}/${FIXTURE_TOTAL_ROWS}`);
        await waitNextTick();
      }

      setFixtureStatus(`Generated ${FIXTURE_TOTAL_ROWS} projects and ${FIXTURE_TOTAL_ROWS} todos`);
    } catch (error) {
      setFixtureStatus(`Failed to generate fixtures: ${String(error)}`);
    } finally {
      setIsGeneratingFixtures(false);
    }
  }, [db, sessionUserId, isGeneratingFixtures]);

  return (
    <>
      <h2>Fixture Generator</h2>
      <p>Generates random fixture data using a dedicated word set.</p>
      <button type="button" onClick={onNavigateTodos}>
        Back to todos
      </button>
      <div>
        <button
          type="button"
          onClick={generateFixtures}
          disabled={!sessionUserId || isGeneratingFixtures}
        >
          {isGeneratingFixtures ? "Generating fixtures..." : "Generate fixtures"}
        </button>
        {fixtureStatus && <p>{fixtureStatus}</p>}
      </div>
      <section aria-label="Random word set">
        <h3>Random words in use</h3>
        <p>{RANDOM_WORDS.join(", ")}</p>
      </section>
    </>
  );
}
