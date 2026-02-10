<script lang="ts">
  import type { co } from 'jazz-tools';
  import type { Message } from '@/lib/schema';
  import { createJazzPlugin } from 'jazz-tools/prosemirror';
  import { baseKeymap } from 'prosemirror-commands';
  import { history } from 'prosemirror-history';
  import { keymap } from 'prosemirror-keymap';
  import { EditorState } from 'prosemirror-state';
  import { EditorView } from 'prosemirror-view';
  import FloatingToolbar from './FloatingToolbar.svelte';
  import { createFloatingToolbarPlugin } from './floatingToolbarPlugin';
  import {
    createEditorKeymap,
    createEditorSchema,
    INITIAL_MENU_STATE,
    type MenuState
  } from './utils';

  interface Props {
    message: co.loaded<typeof Message>;
    onEnter: () => void;
  }

  let { message, onEnter }: Props = $props();

  let editorRef: HTMLDivElement | undefined = $state(undefined);
  let view = $state<EditorView | null>(null);
  let menuState = $state<MenuState>(INITIAL_MENU_STATE);

  function setMenuState(newState: MenuState) {
    menuState = newState;
  }

  $effect(() => {
    if (!editorRef || !message) return;

    const schema = createEditorSchema();
    const jazzPlugin = createJazzPlugin(message.text);
    const customKeymap = createEditorKeymap(schema, onEnter);
    const toolbarPlugin = createFloatingToolbarPlugin(setMenuState);

    const state = EditorState.create({
      schema,
      plugins: [history(), customKeymap, keymap(baseKeymap), jazzPlugin, toolbarPlugin]
    });

    const editorView = new EditorView(editorRef, { state });
    view = editorView;
    editorView.focus();

    return () => editorView.destroy();
  });
</script>

<div class="relative flex-1 min-w-0">
  <div
    bind:this={editorRef}
    role="textbox"
    aria-multiline="true"
    id="messageEditor"
    data-testid="messageEditor"
    class="[&_.ProseMirror]:outline-none [&_.ProseMirror]:prose-sm [&_.ProseMirror]:whitespace-pre-wrap [&_.ProseMirror]:max-w-full [&_.ProseMirror]:break-words w-full max-w-full rounded-md border bg-background p-2"
  ></div>
  <FloatingToolbar {view} {menuState} />
</div>
