"use client";

import { Icon } from "@/components/atoms/Icon";
import { Switch } from "@/components/atoms/Switch";
import { CoFeedVisualizer } from "@components/atoms/covalues/CoFeeds";
import { CoListVisualizer } from "@components/atoms/covalues/CoList";
import { CoMapVisualizer } from "@components/atoms/covalues/CoMap";
import { useMemo, useState } from "react";
import { ListOfPeople, Person, PersonFeed } from "./schema";

export default function Playground() {
  const examplePersonA = useMemo(
    () =>
      Person.create({
        name: "John",
        age: 20,
        height: 180,
        weight: 70,
      }),
    [],
  );

  const examplePersonB = useMemo(
    () =>
      Person.create({
        name: "Jane",
        age: 21,
        height: 170,
        weight: 60,
      }),
    [],
  );
  const examplePersonList = useMemo(
    () => ListOfPeople.create([examplePersonA, examplePersonB]),
    [examplePersonA, examplePersonB],
  );

  const examplePersonFeed = useMemo(() => {
    const feed = PersonFeed.create([]);
    feed.$jazz.push(Person.create({ name: "John", age: 5 }));
    feed.$jazz.push(Person.create({ name: "John", age: 10 }));
    feed.$jazz.push(Person.create({ name: "John", age: 15 }));
    feed.$jazz.push(Person.create({ name: "John", age: 20 }));
    return feed;
  }, []);

  const [showMetadataCoMap, setShowMetadataCoMap] = useState(true);
  const [showDataCoMap, setShowDataCoMap] = useState(true);
  const [showMetaDataCoList, setShowMetaDataCoList] = useState(true);
  const [showDataCoList, setShowDataCoList] = useState(true);
  const [showAllEntries, setShowAllEntries] = useState(true);
  const [showDataCoFeed, setShowDataCoFeed] = useState(true);

  return (
    <div className="p-8">
      <h1 className="text-2xl font-bold mb-4">
        CoValues Visualizer Playground
      </h1>
      <div className="mb-8">
        <h2 className="text-xl font-semibold mb-2">
          <div className="flex items-center gap-2 w-screen">
            <Icon name="comap" className="size-lg" />
            CoMaps
            <div className="flex items-center gap-2 ml-10 w-100">
              <Switch
                size="sm"
                id="showMetadataCoMap"
                checked={showMetadataCoMap}
                onChange={() => setShowMetadataCoMap(!showMetadataCoMap)}
                label="Show Metadata"
              />
              <Switch
                size="sm"
                id="showDataCoMap"
                checked={showDataCoMap}
                onChange={() => setShowDataCoMap(!showDataCoMap)}
                label="Show Data"
              />
            </div>
          </div>
        </h2>
        <div className="flex gap-4">
          <CoMapVisualizer
            instance={examplePersonA}
            showData={showDataCoMap}
            showMetadata={showMetadataCoMap}
          />
        </div>
      </div>
      <div className="mb-8">
        <h2 className="text-xl font-semibold mb-2">
          <div className="flex items-center gap-2">
            <Icon name="colist" className="size-lg" />
            CoLists
            <div className="flex items-center gap-2 ml-10 w-100">
              <Switch
                size="sm"
                id="showMetaDataCoList"
                checked={showMetaDataCoList}
                onChange={() => setShowMetaDataCoList(!showMetaDataCoList)}
                label="Show Metadata"
              />
              <Switch
                size="sm"
                id="showDataCoList"
                checked={showDataCoList}
                onChange={() => setShowDataCoList(!showDataCoList)}
                label="Show Data"
              />
            </div>
          </div>
        </h2>
        <div className="flex gap-4">
          <CoListVisualizer
            instance={examplePersonList}
            showMetadata={showMetaDataCoList}
            showData={showDataCoList}
          />
        </div>
      </div>
      <div className="mb-8">
        <h2 className="text-xl font-semibold mb-2">
          <div className="flex items-center gap-2">
            <Icon name="cofeed" className="size-lg" />
            CoFeeds
            <div className="flex items-center gap-2 ml-10 w-100">
              <Switch
                size="sm"
                id="showAllEntries"
                checked={showAllEntries}
                onChange={() => setShowAllEntries(!showAllEntries)}
                label="Show All Entries"
              />
              <Switch
                size="sm"
                id="showDataCoFeed"
                checked={showDataCoFeed}
                onChange={() => setShowDataCoFeed(!showDataCoFeed)}
                label="Show Data"
              />
            </div>
          </div>
        </h2>
        <div className="flex gap-4">
          <CoFeedVisualizer
            instance={examplePersonFeed}
            showAllEntries={showAllEntries}
            showData={showDataCoFeed}
          />
        </div>
      </div>
    </div>
  );
}
