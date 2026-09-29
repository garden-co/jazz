import { useState } from "react";
import { useAll, useDb } from "jazz-tools/react";
import { Button } from "@astryxdesign/core/Button";
import { CheckboxInput } from "@astryxdesign/core/CheckboxInput";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { Icon } from "@astryxdesign/core/Icon";
import { IconButton } from "@astryxdesign/core/IconButton";
import { List, ListItem } from "@astryxdesign/core/List";
import { SegmentedControl, SegmentedControlItem } from "@astryxdesign/core/SegmentedControl";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { TextInput } from "@astryxdesign/core/TextInput";
import { app } from "../../schema.js";
import { useMe } from "../data/me.js";
import { Page } from "./Page.js";

type Filter = "all" | "open" | "done";

/**
 * Your own checklist: things to pack and remember before a show. Items are
 * private (see permissions.ts), save instantly and sync to your other devices.
 */
export function Checklist() {
  const db = useDb();
  const me = useMe();
  const [title, setTitle] = useState("");
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState<Filter>("all");

  // The query is rebuilt as you type; the subscription follows it live.
  let query = app.checklistItems.where({ ownerAccount: me.account });
  if (search.trim()) query = query.where({ title: { contains: search.trim() } });
  if (filter !== "all") query = query.where({ done: filter === "done" });
  const { data: items } = useAll(query.orderBy("title", "asc"));

  return (
    <Page title="Checklist" description="Your own list for show day. Only you can see it.">
      <form
        onSubmit={(event) => {
          event.preventDefault();
          if (!title.trim()) return;
          db.insert(app.checklistItems, {
            title: title.trim(),
            done: false,
            ownerAccount: me.account,
          });
          setTitle("");
        }}
      >
        <HStack gap={2} vAlign="end">
          <TextInput
            label="New item"
            isLabelHidden
            placeholder="Spare gaffer tape, in-ears, set list…"
            value={title}
            onChange={setTitle}
            width="100%"
          />
          <Button label="Add" type="submit" variant="primary" isDisabled={!title.trim()} />
        </HStack>
      </form>
      <VStack gap={3}>
        <HStack gap={3} vAlign="center" wrap="wrap">
          <TextInput
            label="Filter"
            isLabelHidden
            placeholder="Filter as you type"
            startIcon={<Icon icon="search" size="sm" />}
            value={search}
            onChange={setSearch}
            hasClear
            width="min(100%, 320px)"
          />
          <SegmentedControl
            label="Show"
            value={filter}
            onChange={(next) => setFilter(next as Filter)}
          >
            <SegmentedControlItem value="all" label="All" />
            <SegmentedControlItem value="open" label="Open" />
            <SegmentedControlItem value="done" label="Done" />
          </SegmentedControl>
        </HStack>
        {items && items.length === 0 ? (
          <EmptyState
            isCompact
            title={search || filter !== "all" ? "Nothing matches" : "Your checklist is empty"}
            description={
              search || filter !== "all" ? "Try another filter." : "Add the first thing to bring."
            }
          />
        ) : (
          <List hasDividers density="compact" id="checklist">
            {items?.map((item) => (
              <ListItem
                key={item.id}
                label={
                  <CheckboxInput
                    label={item.title}
                    value={item.done}
                    onChange={(done) => db.update(app.checklistItems, item.id, { done })}
                  />
                }
                endContent={
                  <IconButton
                    label={`Delete ${item.title}`}
                    variant="ghost"
                    size="sm"
                    icon={<Icon icon="close" size="sm" />}
                    onClick={() => db.delete(app.checklistItems, item.id)}
                  />
                }
              />
            ))}
          </List>
        )}
      </VStack>
    </Page>
  );
}
