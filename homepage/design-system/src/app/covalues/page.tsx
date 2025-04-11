"use client";

import { CoFeed } from "jazz-tools";
import { CoFeedVisualizer } from "../components/atoms/covalues/CoFeeds";
import { CoListVisualizer } from "../components/atoms/covalues/CoList";
import { CoMapVisualizer } from "../components/atoms/covalues/CoMap";
import { ListOfPeople, Person, PersonFeed } from "./schema";

export default function Playground() {
  const examplePersonA = Person.create({
    name: "John",
    age: 20,
    height: 180,
    weight: 70,
  });

  const examplePersonB = Person.create({
    name: "Jane",
    age: 21,
    height: 170,
    weight: 60,
  });

  const examplePersonList = ListOfPeople.create([
    examplePersonA,
    examplePersonB,
  ]);

  const examplePersonFeed = PersonFeed.create([]);
  examplePersonFeed.push(Person.create({ name: "John", age: 5 }));
  examplePersonFeed.push(Person.create({ name: "John", age: 10 }));
  examplePersonFeed.push(Person.create({ name: "John", age: 15 }));
  examplePersonFeed.push(Person.create({ name: "John", age: 20 }));

  return (
    <div className="p-8">
      <h1 className="text-2xl font-bold mb-4">
        CoValues Visualizer Playground
      </h1>
      <div className="mb-8">
        <h2 className="text-xl font-semibold mb-2">CoMaps</h2>
        <div className="flex flex-col gap-4">
          <CoMapVisualizer instance={examplePersonA} />
          <CoMapVisualizer instance={examplePersonA} showMetadata={true} />
          <CoMapVisualizer instance={examplePersonA} showData={false} />
        </div>
      </div>
      <div className="mb-8">
        <h2 className="text-xl font-semibold mb-2">CoLists</h2>
        <div className="flex flex-col gap-4">
          <CoListVisualizer instance={examplePersonList} />
          <CoListVisualizer instance={examplePersonList} showMetadata={true} />
          <CoListVisualizer instance={examplePersonList} showData={false} />
        </div>
      </div>
      <div className="mb-8">
        <h2 className="text-xl font-semibold mb-2">CoFeeds</h2>
        <div className="flex flex-col gap-4">
          <CoFeedVisualizer
            instance={examplePersonFeed}
            showAllEntries={true}
          />
          <CoFeedVisualizer
            instance={examplePersonFeed}
            showAllEntries={true}
            showData={false}
          />
        </div>
      </div>
    </div>
  );
}
