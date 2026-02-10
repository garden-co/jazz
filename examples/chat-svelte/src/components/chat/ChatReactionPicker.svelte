<script lang="ts">
  import { SmilePlus } from 'lucide-svelte';
  import { Button } from '@/components/ui/button';
  import * as DropdownMenu from '@/components/ui/dropdown-menu';
  import { Input } from '@/components/ui/input';

  interface Props {
    onPick: (emoji: string) => void;
  }

  let { onPick }: Props = $props();

  let customEmoji = $state('🍉');
  const presets = ['❤️', '👍', '🔥', '😂', '😮', '😢'];

  // This regex is intended to allow users to only enter emojis
  const emojiRegex =
    /^(\p{Extended_Pictographic}|\p{Emoji_Component}|\p{Emoji_Presentation}|\s)+$/u;
</script>

<DropdownMenu.Sub>
  <DropdownMenu.SubTrigger>
    <SmilePlus class="size-4" />
    React
  </DropdownMenu.SubTrigger>
  <DropdownMenu.SubContent class="w-36">
    <DropdownMenu.Label>Quick Reactions</DropdownMenu.Label>
    <div class="flex flex-wrap gap-2 p-2">
      {#each presets as emoji}
        <Button variant="outline" onclick={() => onPick(emoji)}>
          {emoji}
        </Button>
      {/each}
    </div>
    <DropdownMenu.Separator />
    <div class="p-2">
      <label for="customEmoji" class="text-muted-foreground mb-1 block text-xs"> Custom </label>
      <div class="flex gap-2">
        <Input
          id="customEmoji"
          value={customEmoji}
          oninput={(e) => (customEmoji = e.currentTarget.value)}
          class="h-8"
          maxlength={2}
        />
        <Button
          size="sm"
          variant="outline"
          class="h-8 px-2"
          disabled={!customEmoji || !emojiRegex.test(customEmoji)}
          onclick={() => {
            onPick(customEmoji);
            customEmoji = '';
          }}
        >
          Add
        </Button>
      </div>
    </div>
  </DropdownMenu.SubContent>
</DropdownMenu.Sub>
