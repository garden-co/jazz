# Jazz Browser ProseMirror

Core browser-side integration between [ProseMirror](https://prosemirror.net/) and Jazz's collaborative rich text editing.

## Installation

```bash
pnpm install jazz-browser-prosemirror prosemirror-view prosemirror-state prosemirror-schema-basic
```

## Usage

This package provides the core functionality for integrating ProseMirror with Jazz's rich text documents. It handles:

1. Converting between Jazz's rich text format and ProseMirror's document model
2. Applying ProseMirror transactions to Jazz documents
3. Managing document subscriptions and updates

```typescript
import { Document, applyTrToRichText, richTextToProsemirrorDoc } from 'jazz-browser-prosemirror';
import { EditorState } from 'prosemirror-state';
import { EditorView } from 'prosemirror-view';
import { schema } from 'prosemirror-schema-basic';

// Subscribe to document updates
const unsub = Document.subscribe(docID, me, { marks: [{}], text: [] }, (doc) => {
  // Convert Jazz document to ProseMirror format
  const pmDoc = richTextToProsemirrorDoc(doc);
  
  // Update editor state
  editorView.updateState(
    EditorState.create({
      doc: pmDoc,
      schema,
      // ... other state options
    })
  );
});

// Apply ProseMirror changes to Jazz document
editorView.dispatch = (transaction) => {
  applyTrToRichText(doc, transaction);
  // ... update editor state
};

// Clean up
unsub();
```

### API

#### `Document`

A Jazz document type for rich text content.

```ts
import { Document } from "jazz-browser-prosemirror";

const doc = Document.createFromPlainText("Hello, world!", { owner: me });
```

#### `applyTrToRichText(doc: Document, tr: Transaction): void`

Applies a ProseMirror transaction to a Jazz document.

#### `richTextToProsemirrorDoc(doc: Document): Node`

Converts a Jazz document to a ProseMirror document node.

### Framework Integrations

- React: [jazz-react-prosemirror](../jazz-react-prosemirror)

### Example

For a complete example of using this package, see the [Rich Text Example](../../examples/richtext).

## Development

### Running Tests

```bash
pnpm test
```

### Building

```bash
pnpm build
```

## License

MIT