import { useState } from "react";
import { useAll, useDb } from "jazz-tools/react";
import { Badge } from "@astryxdesign/core/Badge";
import { Button } from "@astryxdesign/core/Button";
import { ClickableCard } from "@astryxdesign/core/ClickableCard";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { Grid } from "@astryxdesign/core/Grid";
import { ProgressBar } from "@astryxdesign/core/ProgressBar";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";
import { app } from "../../schema.js";
import { createShow } from "../model/actions.js";
import { useMe } from "../model/me.js";
import { href, navigate } from "../router.js";
import { formatShowDate } from "./format.js";
import { JoinByLink } from "./JoinShow.js";
import { Page } from "./Page.js";
import { ShowDialog } from "./ShowDialog.js";

export function ShowList() {
  const db = useDb();
  const me = useMe();
  const [isCreating, setCreating] = useState(false);
  // Read permissions only return shows you crew on, so this is "my shows".
  const { data: shows } = useAll(
    app.shows.orderBy("date", "asc").include({ tasks: app.tasks.select("status") }),
  );

  const newShowButton = (
    <Button label="New show" variant="primary" onClick={() => setCreating(true)} />
  );

  return (
    <Page title="Shows" actions={newShowButton}>
      {shows && shows.length === 0 ? (
        <EmptyState
          title="No shows yet"
          description="Create a show to plan its stage prep, or open an invite link from a crew chief."
          actions={newShowButton}
        />
      ) : (
        <Grid columns={{ minWidth: 280 }} gap={4}>
          {shows?.map((show) => {
            const done = show.tasks.filter((task) => task.status === "done").length;
            return (
              <ClickableCard key={show.id} label={show.name} href={href.show(show.id)} padding={5}>
                <VStack gap={3}>
                  <VStack gap={1}>
                    <Heading level={3} maxLines={2}>
                      {show.name}
                    </Heading>
                    {show.chiefAccount === me.account && (
                      <HStack>
                        <Badge label="Chief" variant="info" />
                      </HStack>
                    )}
                    <Text color="secondary">
                      {show.venue} · {formatShowDate(show.date)} · doors {show.doors}
                    </Text>
                  </VStack>
                  <ProgressBar
                    label={`${done} of ${show.tasks.length} tasks done`}
                    value={done}
                    max={Math.max(show.tasks.length, 1)}
                  />
                </VStack>
              </ClickableCard>
            );
          })}
        </Grid>
      )}
      <JoinByLink />
      <ShowDialog
        title="New show"
        submitLabel="Create show"
        isOpen={isCreating}
        onOpenChange={setCreating}
        onSubmit={async (input) => {
          const { show } = await createShow(db, me, input);
          navigate(href.show(show.id));
        }}
      />
    </Page>
  );
}
