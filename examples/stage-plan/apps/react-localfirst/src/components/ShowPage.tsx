import { useState } from "react";
import { Pencil } from "lucide-react";
import { useAll, useDb, useOne } from "jazz-tools/react";
import { Badge } from "@astryxdesign/core/Badge";
import { Button } from "@astryxdesign/core/Button";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { Icon } from "@astryxdesign/core/Icon";
import { HStack } from "@astryxdesign/core/Stack";
import { Tab, TabList } from "@astryxdesign/core/TabList";
import { app } from "../../schema.js";
import { updateShow } from "../data/actions.js";
import { useMe } from "../data/me.js";
import { href, navigate, type ShowTab } from "../router.js";
import { ActivityFeed } from "./ActivityFeed.js";
import { Board } from "./Board.js";
import { CrewPanel } from "./CrewPanel.js";
import { formatShowDate } from "./format.js";
import { Loading } from "./Loading.js";
import { Page } from "./Page.js";
import { ShowDialog } from "./ShowDialog.js";
import { TaskDialog } from "./TaskDialog.js";

type ShowPageProps = { showId: string; tab: ShowTab; taskId?: string };

export function ShowPage({ showId, tab, taskId }: ShowPageProps) {
  const db = useDb();
  const me = useMe();
  const [isEditing, setEditing] = useState(false);
  // An empty local result waits for the server, so a fresh device doesn't
  // flash "not on this crew" before the show arrives.
  const { data: show, isLoading } = useOne(app.shows.where({ id: showId }), {
    tier: "local-first-unless-empty",
  });
  const { data: crew = [] } = useAll(
    app.showCrew.where({ showId }).include({ crew: true }).orderBy("role", "asc"),
  );
  const { data: tasks = [] } = useAll(app.tasks.where({ showId }).orderBy("rank", "asc"));

  if (isLoading) return <Loading label="Opening the show" />;
  if (!show) {
    // Outsiders get the same answer as for a show that doesn't exist.
    return (
      <Page title="Show not found">
        <EmptyState
          title="You're not on this show's crew"
          description="Only the crew of a show can see it. Ask the crew chief for an invite link."
          actions={<Button label="Back to shows" href={href.shows()} />}
        />
      </Page>
    );
  }

  const isChief = show.chiefAccount === me.account;
  const openTask = taskId ? tasks.find((task) => task.id === taskId) : undefined;

  return (
    <Page
      title={show.name}
      description={`${show.venue} · ${formatShowDate(show.date)} · doors ${show.doors}`}
      actions={
        <HStack gap={2} vAlign="center">
          <Badge label={isChief ? "Crew chief" : "Crew"} variant={isChief ? "info" : "neutral"} />
          {isChief && (
            <Button
              label="Edit show"
              icon={<Icon icon={Pencil} size="sm" />}
              onClick={() => setEditing(true)}
            />
          )}
        </HStack>
      }
    >
      <TabList
        value={tab}
        onChange={(next) => navigate(href.show(showId, next as ShowTab))}
        hasDivider
      >
        <Tab value="board" label="Board" href={href.show(showId, "board")} />
        <Tab value="crew" label={`Crew (${crew.length})`} href={href.show(showId, "crew")} />
        <Tab value="activity" label="Activity" href={href.show(showId, "activity")} />
      </TabList>

      {tab === "board" && <Board showId={showId} tasks={tasks} crew={crew} />}
      {tab === "crew" && <CrewPanel show={show} crew={crew} />}
      {tab === "activity" && <ActivityFeed query={app.activity.where({ showId })} tasks={tasks} />}

      <TaskDialog
        task={openTask}
        crew={crew}
        isMissing={Boolean(taskId) && !openTask}
        onClose={() => navigate(href.show(showId))}
      />
      <ShowDialog
        title="Edit show"
        submitLabel="Save"
        initial={{ name: show.name, venue: show.venue, date: show.date, doors: show.doors }}
        isOpen={isEditing}
        onOpenChange={setEditing}
        onSubmit={(input) => {
          updateShow(db, show.id, input);
        }}
      />
    </Page>
  );
}
