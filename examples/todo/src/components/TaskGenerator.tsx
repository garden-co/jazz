import { TodoAccount } from "@/1_schema";
import { FormEvent, useState } from "react";
import { useNavigate } from "react-router-dom";
import { generateRandomProject } from "../generate";

export function TaskGenerator() {
  const [isGenerating, setIsGenerating] = useState(false);
  const navigate = useNavigate();

  const handleSubmit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const formData = new FormData(e.currentTarget);
    const numTasks = Math.max(
      1,
      parseInt(formData.get("numTasks") as string) || 1,
    );

    setIsGenerating(true);
    const project = generateRandomProject(numTasks);

    const { root } = await TodoAccount.getMe().ensureLoaded({
      resolve: {
        root: {
          projects: true,
        },
      },
    });

    root.projects.push(project);

    navigate(`/project/${project.id}`);
  };

  return (
    <div className="p-4 border rounded-lg shadow-sm bg-white">
      <h2 className="text-lg font-semibold mb-4">Generate Random Tasks</h2>
      <form onSubmit={handleSubmit} className="flex flex-col gap-4">
        <div className="flex items-center gap-2">
          <label htmlFor="numTasks" className="text-sm font-medium">
            Number of tasks:
          </label>
          <input
            id="numTasks"
            name="numTasks"
            type="number"
            min="1"
            defaultValue={5}
            className="w-20 px-2 py-1 border rounded"
          />
        </div>
        <button
          type="submit"
          disabled={isGenerating}
          className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 disabled:bg-blue-300"
        >
          {isGenerating ? "Generating..." : "Generate Tasks"}
        </button>
      </form>
    </div>
  );
}
