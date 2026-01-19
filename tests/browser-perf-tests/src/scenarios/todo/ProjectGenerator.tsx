import { FormEvent, useState } from "react";
import { generateRandomProject } from "./generate";
import { AppAccount } from "../../schema";

export function ProjectGenerator() {
  const [isGenerating, setIsGenerating] = useState(false);

  const handleSubmit = async (e: FormEvent<HTMLFormElement>) => {
    if (isGenerating) return;
    e.preventDefault();
    const formData = new FormData(e.currentTarget);
    const numTasks = Math.max(
      1,
      parseInt(formData.get("numTasks") as string) || 1,
    );

    setIsGenerating(true);
    const project = generateRandomProject(numTasks);

    const { root } = await AppAccount.getMe().$jazz.ensureLoaded({
      resolve: {
        root: {
          projects: true,
        },
      },
    });

    root.projects.$jazz.push(project.value);

    await project.done;

    setIsGenerating(false);
  };

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "row",
        alignItems: "center",
        justifyContent: "space-between",
        padding: "1.5rem",
        border: "1px solid #e5e7eb",
        borderRadius: "8px",
        backgroundColor: "#f9fafb",
      }}
    >
      <h2>Generate Random Tasks</h2>
      <form
        onSubmit={handleSubmit}
        style={{
          display: "flex",
          flexDirection: "row",
          gap: "1rem",
          alignItems: "end",
        }}
      >
        <div
          style={{
            display: "flex",
            flexDirection: "row",
            alignItems: "center",
            gap: "0.5rem",
          }}
        >
          <label
            htmlFor="numTasks"
            style={{
              fontSize: "0.875rem",
              fontWeight: "500",
              color: "#374151",
            }}
          >
            Number of tasks:
          </label>
          <input
            id="numTasks"
            name="numTasks"
            type="number"
            min="1"
            defaultValue={1000}
            style={{
              padding: "0.5rem",
              border: "1px solid #d1d5db",
              borderRadius: "4px",
              fontSize: "0.875rem",
              width: "80px",
            }}
          />
        </div>
        <button
          type="submit"
          disabled={isGenerating}
          style={{
            padding: "0.5rem 1rem",
            backgroundColor: isGenerating ? "#9ca3af" : "#3b82f6",
            color: "white",
            border: "none",
            borderRadius: "4px",
            fontSize: "0.875rem",
            fontWeight: "500",
            cursor: isGenerating ? "not-allowed" : "pointer",
          }}
        >
          {isGenerating ? "Generating..." : "Generate Tasks"}
        </button>
      </form>
    </div>
  );
}
