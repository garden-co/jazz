"use client";
import { useState } from "react";

export function IssueTrackerPreview() {
  const [estimate, setEstimate] = useState(5);
  const [backlog, setBacklog] = useState("backlog");
  return (
    <div className="p-3 md:-mx-3 rounded border border-stone-100 bg-white dark:bg-black not-prose">
      <div className="grid grid-cols-6 text-sm border-r border-b [&>*]:p-2 [&>*]:border-l [&>*]:border-t">
        <input type="text" value={"Buy terrarium"} />
        <input
          type="text"
          className="col-span-3"
          value={"Make sure it's big enough for 10 snails."}
        />
        <label className="flex">
          Estimate:{" "}
          <input
            type="number"
            value={estimate}
            onChange={(e) => setEstimate(Number(e.target.value))}
            className="text-right min-w-0"
          />
        </label>
        <select value={backlog} onChange={(e) => setBacklog(e.target.value)}>
          <option value="backlog">Backlog</option>
          <option value="in progress">In Progress</option>
          <option value="done">Done</option>
        </select>
      </div>
    </div>
  );
}
