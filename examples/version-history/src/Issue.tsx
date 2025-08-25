import { CoPlainText, Loaded } from "jazz-tools";
import { Issue } from "./schema";

export function IssueComponent({ issue }: { issue: Loaded<typeof Issue> }) {
  return (
    <div className="flex flex-col gap-5">
      <label className="flex flex-col gap-2">
        Title
        <input
          type="text"
          value={issue.title}
          onChange={(event) => {
            issue.$jazz.set("title", event.target.value);
          }}
        />
      </label>

      <label className="flex flex-col gap-2">
        Description
        <textarea
          value={`${issue.description}`}
          onChange={(event) => {
            issue.$jazz.set(
              "description",
              CoPlainText.create(event.target.value, issue.$jazz.owner),
            );
          }}
        />
      </label>
      <div className="grid grid-cols-2 gap-8">
        <label className="flex gap-2 items-center">
          Estimate:
          <input
            type="number"
            value={issue.estimate}
            onChange={(event) => {
              issue.$jazz.set("estimate", Number(event.target.value));
            }}
          />
        </label>

        <label className="flex gap-2 items-center">
          Status
          <select
            className="flex-1"
            value={issue.status}
            onChange={(event) => {
              issue.$jazz.set(
                "status",
                event.target.value as "backlog" | "in progress" | "done",
              );
            }}
          >
            <option value="backlog">Backlog</option>
            <option value="in progress">In Progress</option>
            <option value="done">Done</option>
          </select>
        </label>
      </div>
    </div>
  );
}
