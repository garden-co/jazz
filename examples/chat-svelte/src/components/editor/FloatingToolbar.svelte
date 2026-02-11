<script lang="ts">
  import type { EditorView } from 'prosemirror-view';
  import { toggleMark } from 'prosemirror-commands';
  import { Button } from '@/components/ui/button';
  import { ButtonGroup } from '@/components/ui/button-group';
  import { type MenuState, TOOLBAR_BUTTONS } from './utils';

  interface Props {
    view: EditorView | null;
    menuState: MenuState;
  }

  let { view, menuState }: Props = $props();

  function handleToggle(markName: string) {
    if (!view) return;
    const mark = view.state.schema.marks[markName];
    if (mark) {
      toggleMark(mark)(view.state, view.dispatch);
      view.focus();
    }
  }
</script>

{#if menuState.show && view}
  <div
    tabindex={0}
    role="toolbar"
    style:position="fixed"
    style:top="{menuState.top}px"
    style:left="{menuState.left}px"
    style:transform="translateX(-50%)"
    class="z-50 rounded-sm border bg-background shadow-md overflow-hidden"
    onmousedown={(evt) => evt.preventDefault()}
  >
    <ButtonGroup>
      {#each TOOLBAR_BUTTONS as { icon: Icon, markName }}
        <Button
          variant={menuState.activeMarks.includes(markName) ? 'default' : 'secondary'}
          onclick={() => handleToggle(markName)}
          aria-label={markName}
        >
          <Icon />
        </Button>
      {/each}
    </ButtonGroup>
  </div>
{/if}
