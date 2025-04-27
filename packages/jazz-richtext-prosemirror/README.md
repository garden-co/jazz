# Jazz ProseMirror Rich-text Integration

Integration between [ProseMirror](https://prosemirror.net/) and Jazz's collaborative text value `CoRichText`, enabling real-time collaborative rich text editing in your applications.

## Installation

```bash
pnpm add jazz-richtext-prosemirror prosemirror-view prosemirror-state prosemirror-schema-basic
# or
npm install jazz-richtext-prosemirror prosemirror-view prosemirror-state prosemirror-schema-basic
# or
yarn add jazz-richtext-prosemirror prosemirror-view prosemirror-state prosemirror-schema-basic
```

## Features

- **Bidirectional Synchronization**: Changes in ProseMirror automatically update Jazz's CoRichText and vice versa
- **Conflict Resolution**: Uses Jazz's CRDT algorithms to handle concurrent edits gracefully
- **Rich Text Support**: Full support for formatting, links, and other rich text features
- **Efficient Updates**: Only sends minimal changes between components

## Basic Usage

```tsx
import { useAccount } from "jazz-react";
import { createJazzPlugin } from "jazz-richtext-prosemirror";
import { exampleSetup } from "prosemirror-example-setup";
import { schema } from "prosemirror-schema-basic";
import { EditorState } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import { useEffect, useRef } from "react";

function RichTextEditor() {
  // Access the user account with the bio field
  const { me } = useAccount({ resolve: { profile: true } });
  const editorRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);

  useEffect(() => {
    if (!me || !editorRef.current || !me.profile.bio) return;

    // Create the Jazz plugin with the CoRichText instance from the account
    const jazzPlugin = createJazzPlugin(me.profile.bio);

    // Set up ProseMirror with the Jazz plugin
    if (!viewRef.current) {
      viewRef.current = new EditorView(editorRef.current, {
        state: EditorState.create({
          schema,
          plugins: [...exampleSetup({ schema }), jazzPlugin],
        }),
      });
    }

    return () => {
      if (viewRef.current) {
        viewRef.current.destroy();
        viewRef.current = null;
      }
    };
  }, [me?.id, me?.profile.bio?.id]);

  if (!me) return null;

  return <div ref={editorRef} className="border rounded p-2" />;
}
```

## API Reference

### `createJazzPlugin(coRichText, config?)`

Creates a ProseMirror plugin that syncs with a Jazz CoRichText instance.

**Parameters:**

- `coRichText`: The CoRichText instance to synchronize with
- `config` (optional): Configuration options
  - `showDecorations`: Whether to show caret and selection decorations (default: false)

**Returns:**

- A ProseMirror plugin instance that can be added to your editor

## How it works

This package handles:

1. **Conversion**: Transforms between HTML (used by Jazz) and ProseMirror's document model
2. **Change Tracking**: Detects changes in both systems and applies them to the other
3. **Conflict Resolution**: Leverages Jazz's CRDT algorithms for handling concurrent edits
4. **Efficiency**: Only updates the necessary parts of documents when changes occur

Under the hood, it uses:

- `htmlToProseMirror` and `proseMirrorToHtml` to convert between formats
- ProseMirror transactions to track editor changes
- `CoRichText.applyDiff()` for efficient updates

## Complete Example

See the [richtext example](https://github.com/gardencmp/jazz/tree/main/examples/richtext) for a complete implementation demonstrating:

- Side-by-side plaintext and rich text editing
- Real-time collaboration
- Persistent storage

## License

MIT
