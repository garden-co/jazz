# Jazz React ProseMirror

React component for integrating [ProseMirror](https://prosemirror.net/) with Jazz's collaborative rich text editing. If you're not using React, you can use the [jazz-browser-prosemirror](../jazz-browser-prosemirror) package instead.

## Installation

```bash
pnpm install jazz-react-prosemirror prosemirror-view prosemirror-state prosemirror-schema-basic prosemirror-example-setup
```

## Usage

The package provides a `DocumentComponent` that wraps ProseMirror and handles bidirectional synchronization between Jazz's rich text state and ProseMirror's editor state.

```tsx
import { DocumentComponent } from 'jazz-react-prosemirror';
import { Document } from 'jazz-browser-prosemirror';
import { ID } from 'jazz-tools';

function Editor({ docID }: { docID: ID<Document> }) {
  return (
    <DocumentComponent 
      docID={docID}
      onReady={() => console.log('Editor ready')}
    />
  );
}
```

### Features

- Real-time collaborative editing
- Automatic synchronization between Jazz and ProseMirror
- Focus preservation during updates
- Clean unmounting and resource cleanup
- Support for rich text features:
  - Bold and italic
  - Paragraphs and text blocks
  - And more via ProseMirror's extensible schema

### Props

- `docID: ID<Document>` - The ID of the Jazz document to edit
- `onReady?: () => void` - Optional callback that fires when the editor is fully initialized

### Example

For a complete example of using this component in a React application, see the [Rich Text Example](../../examples/richtext).

## Development

### Running Tests

```bash
npm test
```

### Building

```bash
npm run build
```

## License

MIT 