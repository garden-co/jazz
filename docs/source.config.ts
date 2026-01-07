import { defineDocs, defineConfig } from 'fumadocs-mdx/config';
// import { remarkDocGen, fileGenerator } from 'fumadocs-docgen';

export const docs = defineDocs({
  dir: 'content/docs',
});

export default defineConfig({
  // mdxOptions: {
  //   remarkPlugins: [
  //     // Enable including code snippets from external files
  //     // Usage: ```json doc-gen:file
  //     // { "file": "../../examples/react-app/src/App.tsx", "codeblock": { "lang": "tsx" } }
  //     // ```
  //     [remarkDocGen, { generators: [fileGenerator()] }],
  //   ],
  // },
});
