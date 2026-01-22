import {
  FileStream,
  ImageDefinition,
  co,
  z,
  setDefaultValidationMode,
} from "jazz-tools";
import {
  Issue,
  Organization,
  Project,
  type ReactionType,
  ReactionsList,
} from "./schema";

setDefaultValidationMode("strict");

const projectsData: {
  name: string;
  description: string;
  issues: {
    title: string;
    status: "open" | "closed";
    labels: string[];
    reactions?: ReactionType[];
    file?: boolean;
    image?: boolean;
  }[];
}[] = [
  {
    name: "Jazz",
    description: "Jazz is a framework for building collaborative apps.",
    issues: [
      {
        title: "Issue 1",
        status: "open",
        labels: [
          "bug",
          "feature",
          "enhancement",
          "documentation",
          "homepage",
          "help needed",
          "requested",
          "blocked",
          "high priority",
          "urgent",
        ],
        reactions: ["thumb-up"],
        image: true,
        file: true,
      },
      { title: "Issue 2", status: "closed", labels: ["bug"] },
      { title: "Issue 3", status: "open", labels: ["feature", "enhancement"] },
    ],
  },
  {
    name: "Waffle",
    description: "Start waffling",
    issues: [],
  },
  {
    name: "Garden",
    description: "Grow your garden",
    issues: [],
  },
  {
    name: "Tilescape",
    description: "",
    issues: [],
  },
];

export const createFile = () => {
  const file = FileStream.create();
  file.start({ mimeType: "image/jpeg" });
  file.push(new Uint8Array([1, 2, 3]));
  file.end();
  return file;
};

export const createImage = () => {
  return ImageDefinition.create({
    original: FileStream.create(),
    originalSize: [1920, 1080],
    progressive: false,
    placeholderDataURL: "data:image/jpeg;base64,...",
  });
};

export const createOrganization = () => {
  return Organization.create({
    name: "Garden Computing",
    image: ImageDefinition.create({
      original: FileStream.create(),
      progressive: false,
      originalSize: [1920, 1080],
      placeholderDataURL: "data:image/jpeg;base64,...",
    }),
    projects: co.list(Project).create(
      projectsData.map((project) =>
        Project.create({
          name: project.name,
          description: project.description,
          issues: co.list(Issue).create(
            project.issues.map((issue) =>
              Issue.create({
                title: issue.title,
                status: issue.status,
                labels: co.list(z.string()).create(issue.labels),
                reactions: ReactionsList.create(issue.reactions || []),
                file: issue.file ? createFile() : undefined,
                image: issue.image ? createImage() : undefined,
              }),
            ),
          ),
        }),
      ),
    ),
  });
};
