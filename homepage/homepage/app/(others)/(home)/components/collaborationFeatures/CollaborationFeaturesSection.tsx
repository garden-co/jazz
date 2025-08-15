import { Card } from "@garden-co/design-system/src/components/atoms/Card";
import { GappedGrid } from "@garden-co/design-system/src/components/molecules/GappedGrid";
import { Prose } from "@garden-co/design-system/src/components/molecules/Prose";
import { SectionHeader } from "@garden-co/design-system/src/components/molecules/SectionHeader";
import CollaborationInvite from "./CollaborationInvite.mdx";
import CollaborationPrivate from "./CollaborationPrivate.mdx";
import CollaborationPublic from "./CollaborationPublic.mdx";

const data = [
  {
    title: "Private",
    description: "By default, CoValues are visible only to you.",
    codeSample: CollaborationPrivate,
  },
  {
    title: "Shared",
    description: "Use Groups for precise access control.",
    codeSample: CollaborationPublic,
  },
  {
    title: "By Invitation",
    description: "Create links that allow anyone to join.",
    codeSample: CollaborationInvite,
  },
];

export function CollaborationFeaturesSection() {
  return (
    <div>
      <SectionHeader
        kicker="Role-based permissions"
        title="Making secure collaboration the default"
        slogan={
          <>
            Every CoValue belongs to a <code>Group</code>, where you can assign
            roles to users to control access &mdash; all from the client.
          </>
        }
      ></SectionHeader>

      <GappedGrid>
        {data.map(({ title, description, codeSample: CodeSample }) => (
          <Card
            className="col-span-2 pt-4 px-4 flex flex-col gap-3"
            key={title}
          >
            <div>
              <h3 className="text-stone-900 font-medium md:text-base dark:text-stone-100 mb-2">
                {title}
              </h3>
              <Prose>
                <p>{description}</p>
              </Prose>
            </div>
            <pre className="flex-1 text-sm border-t border-x rounded-t-lg bg-stone-50 dark:bg-stone-925">
              <CodeSample />
            </pre>
          </Card>
        ))}
      </GappedGrid>
    </div>
  );
}
