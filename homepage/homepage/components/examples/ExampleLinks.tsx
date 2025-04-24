"use client";

import { Example } from "@/content/example";
import { InterpolateInCode } from "@/mdx-components";
import { DialogDescription } from "@headlessui/react";
import { Button } from "@garden-co/design-system/src/components/atoms/Button";
import { CodeGroup } from "@garden-co/design-system/src/components/molecules/CodeGroup";
import {
  Dialog,
  DialogActions,
  DialogBody,
  DialogTitle,
} from "@garden-co/design-system/src/components/organisms/Dialog";
import { useState } from "react";
import CreateJazzApp from "./CreateJazzApp.mdx";

export function ExampleLinks({ example }: { example: Example }) {
  const { slug, demoUrl } = example;
  const githubUrl = `https://github.com/gardencmp/jazz/tree/main/examples/${slug}`;
  const [isOpen, setIsOpen] = useState(false);

  return (
    <>
      <div className="flex gap-2">
        <Button variant="secondary" size="sm" onClick={() => setIsOpen(true)}>
          Use as template
        </Button>
        <Button href={githubUrl} newTab variant="secondary" size="sm">
          <span className="md:hidden">Code</span>
          <span className="hidden md:inline">View code</span>
        </Button>

        {demoUrl && (
          <Button href={demoUrl} newTab variant="secondary" size="sm">
            <span className="md:hidden">Demo</span>
            <span className="hidden md:inline">View demo</span>
          </Button>
        )}
      </div>

      <Dialog onClose={() => setIsOpen(false)} open={isOpen}>
        <DialogTitle>Use {example.name} example as a template</DialogTitle>
        <DialogBody>
          <div className="mb-6 aspect-[16/9] overflow-hidden w-full rounded-md bg-white border dark:bg-stone-925 sm:aspect-[2/1] md:aspect-[3/2]">
            {example.illustration}
          </div>
          <p className="mb-3">
            Generate a new Jazz app by running the command below.
          </p>
          <CodeGroup>
            <CreateJazzApp
              components={InterpolateInCode({
                $EXAMPLE: example.slug,
              })}
            />
          </CodeGroup>
        </DialogBody>
        <DialogActions>
          <Button onClick={() => setIsOpen(false)} variant="secondary">
            Cancel
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
