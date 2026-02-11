<script lang="ts">
  import { AccountCoState, CoState } from 'jazz-tools/svelte';
  import { Eraser, Pencil } from 'lucide-svelte';
  import { Button } from '@/components/ui/button';
  import { Canvas as CanvasSchema, ChatAccount, type Point, type Stroke } from '@/lib/schema';
  import CollaboratorBadge from '@/components/canvas/CollaboratorBadge.svelte';
  import {
    colorFromAccountId,
    debounce,
    ERASER_WIDTH,
    getLogicalPoint,
    renderCanvasFrame,
    STROKE_WIDTH
  } from '@/components/canvas/utils';

  const DRAW_BROADCAST_DEBOUNCE_MS = 16;

  interface Props {
    canvasId: string;
    height?: string;
    showControls?: boolean;
    class?: string;
  }

  let { canvasId, height = '', showControls = true, class: className = '' }: Props = $props();

  const account = new AccountCoState(ChatAccount, { resolve: { profile: true } });
  const me = $derived(account.current);
  const canvasState = $derived(new CoState(CanvasSchema, canvasId));
  const canvas = $derived(canvasState.current);

  let containerEl: HTMLDivElement | undefined = $state(undefined);
  let canvasRefs = $state<Record<string, HTMLCanvasElement>>({});
  let drawing = $state(false);
  let currentStroke = $state<Stroke | null>(null);
  let erasePoint = $state<Point | null>(null);
  let mode = $state<'draw' | 'erase'>('draw');

  const myAccountId = $derived(me.$isLoaded ? me.$jazz.id : undefined);
  const myColor = $derived(myAccountId ? colorFromAccountId(myAccountId) : '#000000');

  const strokesByAccount = $derived.by(() => {
    const out: Record<string, Stroke[]> = {};
    if (canvas.$isLoaded) {
      for (const [accountId, strokes] of Object.entries(canvas)) {
        out[accountId] = strokes;
      }
    }
    if (myAccountId && !out[myAccountId]) {
      out[myAccountId] = [];
    }
    return out;
  });

  const accountIds = $derived(Object.keys(strokesByAccount).sort());

  function assignRef(id: string, el: HTMLCanvasElement | undefined) {
    if (el) canvasRefs = { ...canvasRefs, [id]: el };
    else {
      const next = { ...canvasRefs };
      delete next[id];
      canvasRefs = next;
    }
  }

  function canvasRefAction(node: HTMLCanvasElement, id: string) {
    assignRef(id, node);
    return {
      destroy() {
        assignRef(id, undefined);
      }
    };
  }

  function renderAll() {
    for (const accountId of accountIds) {
      const cvs = canvasRefs[accountId];
      if (!cvs) continue;
      const isMe = accountId === myAccountId;
      const strokes = strokesByAccount[accountId] ?? [];
      renderCanvasFrame(cvs, strokes, {
        inProgressStroke: isMe ? currentStroke : null,
        erasePoint: isMe ? erasePoint : null
      });
    }
  }

  const debouncedUpdater = $derived(
    debounce((accountId: string, newStrokes: Stroke[]) => {
      if (canvas.$isLoaded) {
        canvas.$jazz.set(accountId, newStrokes);
      }
    }, DRAW_BROADCAST_DEBOUNCE_MS)
  );

  $effect(() => {
    if (!containerEl || !canvas.$isLoaded) return;
    const container = containerEl;
    const observer = new ResizeObserver(() => {
      const width = container.clientWidth;
      const height = container.clientHeight;
      const dpr = window.devicePixelRatio || 1;
      const physicalWidth = Math.floor(width * dpr);
      const physicalHeight = Math.floor(height * dpr);
      for (const cvs of Object.values(canvasRefs)) {
        if (!cvs) continue;
        if (cvs.width !== physicalWidth || cvs.height !== physicalHeight) {
          cvs.width = physicalWidth;
          cvs.height = physicalHeight;
          renderAll();
        }
      }
    });
    observer.observe(container);
    return () => observer.disconnect();
  });

  $effect(() => {
    renderAll();
  });

  function handlePointerDown(e: PointerEvent) {
    if (!myAccountId || (e.button !== 0 && e.pointerType !== 'touch')) return;
    const target = e.currentTarget as HTMLCanvasElement;
    e.preventDefault();
    target.setPointerCapture(e.pointerId);
    drawing = true;
    const point = getLogicalPoint(e.clientX, e.clientY, target);
    currentStroke = {
      id: crypto.randomUUID(),
      points: [point],
      color: mode === 'draw' ? myColor : '#ffffff',
      width: mode === 'draw' ? STROKE_WIDTH : ERASER_WIDTH,
      createdAt: Date.now()
    };
    if (mode === 'erase') erasePoint = point;
  }

  function handlePointerMove(e: PointerEvent) {
    if (!drawing || !currentStroke) return;
    const target = e.currentTarget as HTMLCanvasElement;
    e.preventDefault();
    const point = getLogicalPoint(e.clientX, e.clientY, target);
    currentStroke = {
      ...currentStroke,
      points: [...currentStroke.points, point]
    };
    if (mode === 'erase') erasePoint = point;
    if (myAccountId && canvas.$isLoaded) {
      const myStrokes = strokesByAccount[myAccountId] || [];
      debouncedUpdater(myAccountId, [...myStrokes, currentStroke]);
    }
  }

  function handlePointerUp(e: PointerEvent) {
    if (!drawing) return;
    const target = e.currentTarget as HTMLCanvasElement;
    drawing = false;
    target.releasePointerCapture(e.pointerId);
    if (currentStroke && myAccountId && canvas.$isLoaded) {
      const myStrokes = strokesByAccount[myAccountId] || [];
      canvas.$jazz.set(myAccountId, [...myStrokes, currentStroke]);
    }
    currentStroke = null;
    erasePoint = null;
  }

  function clearMyStrokes() {
    if (myAccountId && canvas.$isLoaded) {
      canvas.$jazz.set(myAccountId, []);
    }
  }
</script>

<section
  class="bg-muted text-muted-foreground mt-1 rounded-sm p-2 {className}"
  onpointerdown={(e) => e.stopPropagation()}
>
  {#if showControls}
    <header class="flex flex-wrap items-center justify-between gap-2 pb-2">
      <div class="flex items-center gap-1 rounded-md border bg-background p-1 shadow-sm">
        <Button
          variant={mode === 'draw' ? 'default' : 'outline'}
          size="sm"
          onclick={() => (mode = 'draw')}
        >
          <Pencil class="size-4" />
          Draw
        </Button>
        <Button
          variant={mode === 'erase' ? 'default' : 'outline'}
          size="sm"
          onclick={() => (mode = 'erase')}
        >
          <Eraser class="size-4" />
          Erase
        </Button>
      </div>
      <Button variant="outline" size="sm" onclick={clearMyStrokes}>Clear my strokes</Button>
    </header>
  {/if}

  <div
    bind:this={containerEl}
    class="relative w-full aspect-4/3 overflow-hidden rounded-md border border-dashed border-stone-300 bg-white shadow-inner {height} {mode ===
    'draw'
      ? 'cursor-crosshair'
      : 'cursor-auto'}"
  >
    {#each accountIds as accountId (accountId)}
      {@const isMe = accountId === myAccountId}
      <canvas
        use:canvasRefAction={accountId}
        class="absolute inset-0 size-full touch-none mix-blend-multiply {!isMe
          ? 'pointer-events-none'
          : 'z-10'}"
        onpointerdown={isMe ? handlePointerDown : undefined}
        onpointermove={isMe ? handlePointerMove : undefined}
        onpointerup={isMe ? handlePointerUp : undefined}
        onpointerleave={isMe ? handlePointerUp : undefined}
        data-testid="canvas"
      ></canvas>
    {/each}
  </div>

  <div class="mt-2 flex flex-wrap gap-2 text-sm">
    {#each accountIds as accountId (accountId)}
      <CollaboratorBadge {accountId} color={colorFromAccountId(accountId)} />
    {/each}
  </div>
</section>
