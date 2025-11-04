#!/usr/bin/env node
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const componentsDir = path.join(__dirname, "../src/components");
const files = fs.readdirSync(componentsDir);

const storyTemplate = (
  fileName: string,
  componentName: string,
  exportName: string,
) => `import type { Meta, StoryObj } from "@storybook/react";
${getImports(fileName, exportName)}

const meta: Meta<typeof ${exportName}> = {
  title: "${getStoryTitle(fileName)}",
  component: ${exportName},
  tags: ["autodocs"],
};

export default meta;
type Story = StoryObj<typeof ${exportName}>;

export const Default: Story = ${getStoryConfig(fileName, exportName)};
`;

// Determine the story title/category
function getStoryTitle(fileName: string): string {
  const formComponents = [
    "input",
    "textarea",
    "checkbox",
    "select",
    "radio-group",
    "radio",
    "switch",
    "label",
    "form",
    "slider",
  ];

  if (formComponents.includes(fileName)) {
    return `jazz-ui/Forms/${fileName}`;
  }

  return `jazz-ui/${fileName}`;
}

// Get imports needed for the story
function getImports(fileName: string, exportName: string): string {
  const compositeImports: Record<string, string> = {
    accordion: `import { Accordion, AccordionItem, AccordionTrigger, AccordionContent } from "./${fileName}.js";`,
    tabs: `import { Tabs, TabsList, TabsTrigger, TabsContent } from "./${fileName}.js";`,
  };

  return (
    compositeImports[fileName] ||
    `import { ${exportName} } from "./${fileName}.js";`
  );
}

// Get story configuration (args or render)
function getStoryConfig(fileName: string, exportName: string): string {
  // Complex composite components need render functions
  const compositeRenders: Record<string, string> = {
    accordion: `{
  render: (args) => (
    <Accordion type="single" collapsible {...args}>
      <AccordionItem value="item-1">
        <AccordionTrigger>Is it accessible?</AccordionTrigger>
        <AccordionContent>
          Yes. It adheres to the WAI-ARIA design pattern.
        </AccordionContent>
      </AccordionItem>
      <AccordionItem value="item-2">
        <AccordionTrigger>Is it styled?</AccordionTrigger>
        <AccordionContent>
          Yes. It comes with default styles that you can customize.
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  ),
}`,
    tabs: `{
  render: (args) => (
    <Tabs defaultValue="tab1" {...args}>
      <TabsList>
        <TabsTrigger value="tab1">Tab 1</TabsTrigger>
        <TabsTrigger value="tab2">Tab 2</TabsTrigger>
      </TabsList>
      <TabsContent value="tab1">Content for Tab 1</TabsContent>
      <TabsContent value="tab2">Content for Tab 2</TabsContent>
    </Tabs>
  ),
}`,
  };

  if (compositeRenders[fileName]) {
    return compositeRenders[fileName];
  }

  // Simple components that just need children
  const needsChildren = ["button", "label", "badge", "alert"];

  if (needsChildren.includes(fileName)) {
    return `{
  args: { children: "${exportName}" },
}`;
  }

  return "{ args: {} }";
}

// Map file names to their actual export names (for edge cases)
function getExportName(fileName: string, componentName: string): string {
  const customExports: Record<string, string> = {
    sonner: "Toaster",
  };
  return customExports[fileName] || componentName;
}

// Generate story files
files.forEach((file) => {
  if (file.endsWith(".tsx") && !file.includes(".stories")) {
    const fileName = file.replace(".tsx", "");
    const componentName = fileName
      .split("-")
      .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
      .join("");

    const exportName = getExportName(fileName, componentName);

    const storyPath = path.join(componentsDir, `${fileName}.stories.tsx`);

    // Always regenerate to keep stories in sync
    fs.writeFileSync(
      storyPath,
      storyTemplate(fileName, componentName, exportName),
    );
    console.log(`✓ Generated ${fileName}.stories.tsx`);
  }
});

console.log("\n✅ Story generation complete!");
