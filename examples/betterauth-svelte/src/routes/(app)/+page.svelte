<script lang="ts">
  import jazzLogo from "$lib/assets/jazz-logo.svg";
  import jazzIcon from "$lib/assets/jazz.svg";

  import { Button } from "$lib/components/ui/button";
  import { Account } from "jazz-tools";
  import { AccountCoState } from "jazz-tools/svelte";
  import {
    AppWindowMacIcon,
    FileTextIcon,
    GlobeIcon,
    WrenchIcon,
  } from "@lucide/svelte";
  const me = new AccountCoState(Account, { resolve: { profile: {} } });
</script>

{#if me.current.$isLoaded}
  <div class="grow flex flex-col items-center justify-center">
    <main class="flex flex-col gap-8 row-start-2 grow justify-center">
      <img src={jazzLogo} alt="Jazz logo" width={180} height={38} />
      <p class="text-sm/6 text-center sm:text-left">
        Signed in as{" "}
        <span
          class="bg-black/5 dark:bg-white/6 px-1 py-0.5 rounded font-mono font-semibold"
        >
          {me.current.profile.name}
        </span>{" "}
        with id{" "}
        <span
          class="bg-black/5 dark:bg-white/6 px-1 py-0.5 rounded font-mono font-semibold"
        >
          {me.current.$jazz.id}
        </span>
      </p>

      <div class="flex gap-4 items-center flex-row">
        <Button
          size="lg"
          href="https://jazz.tools/docs"
          target="_blank"
          rel="noopener noreferrer"
          class="flex gap-1 items-center"
        >
          <img src={jazzIcon} alt="" width={20} height={20} />
          Start building
        </Button>

        <Button
          variant="secondary"
          size="lg"
          href="https://jazz.tools/docs"
          target="_blank"
          rel="noopener noreferrer"
          class="flex gap-1 items-center"
        >
          <FileTextIcon class="size-4" />
          Read the docs
        </Button>
      </div>
    </main>

    <footer class="flex gap-4 py-8">
      <Button
        variant="ghost"
        href="https://jazz.tools/docs"
        target="_blank"
        rel="noopener noreferrer"
        class="flex gap-1 items-center"
      >
        <FileTextIcon class="size-4" />
        Docs
      </Button>
      <Button
        variant="ghost"
        href="https://jazz.tools/examples"
        target="_blank"
        rel="noopener noreferrer"
        class="flex gap-1 items-center"
      >
        <AppWindowMacIcon class="size-4" />
        Examples
      </Button>

      <Button
        variant="ghost"
        href="https://jazz.tools/status"
        target="_blank"
        rel="noopener noreferrer"
        class="flex gap-1 items-center"
      >
        <GlobeIcon class="size-4" />
        Status
      </Button>

      <Button
        variant="ghost"
        href="https://jazz.tools/showcase"
        target="_blank"
        rel="noopener noreferrer"
        class="flex gap-1 items-center"
      >
        <WrenchIcon class="size-4" />
        Built with Jazz
      </Button>
    </footer>
  </div>
{/if}

<!--
export default function Home() {

  if (!me.$isLoaded) {
    return null;
  }

  return (
    <div class="grow flex flex-col items-center justify-center">
      <main class="flex flex-col gap-8 row-start-2 grow justify-center">
        <Image
          src="/jazz-logo.svg"
          alt="Jazz logo"
          width={180}
          height={38}
          priority
        />
        <p class="text-sm/6 text-center sm:text-left">
          Signed in as{" "}
          <span class="bg-black/[.05] dark:bg-white/[.06] px-1 py-0.5 rounded font-mono font-semibold">
            {me.profile.name}
          </span>{" "}
          with id{" "}
          <span class="bg-black/[.05] dark:bg-white/[.06] px-1 py-0.5 rounded font-mono font-semibold">
            {me.$jazz.id}
          </span>
        </p>

        <div class="flex gap-4 items-center flex-row">
          <Button asChild size="lg">
            <a
              href="https://jazz.tools/docs"
              target="_blank"
              rel="noopener noreferrer"
            >
              <Image src="/jazz.svg" alt="" width={20} height={20} />
              Start building
            </a>
          </Button>

          <Button asChild variant="secondary" size="lg">
            <a
              href="https://jazz.tools/docs"
              target="_blank"
              rel="noopener noreferrer"
            >
              <FileTextIcon class="size-4" />
              Read the docs
            </a>
          </Button>
        </div>
      </main>

      <footer class="flex gap-4 py-8">
        <Button asChild variant="ghost">
          <a
            href="https://jazz.tools/docs"
            target="_blank"
            rel="noopener noreferrer"
          >
            <FileTextIcon class="size-4" />
            Docs
          </a>
        </Button>
        <Button asChild variant="ghost">
          <a
            href="https://jazz.tools/examples"
            target="_blank"
            rel="noopener noreferrer"
          >
            <AppWindowMacIcon class="size-4" />
            Examples
          </a>
        </Button>

        <Button asChild variant="ghost">
          <a
            href="https://jazz.tools/status"
            target="_blank"
            rel="noopener noreferrer"
          >
            <GlobeIcon class="size-4" />
            Status
          </a>
        </Button>

        <Button asChild variant="ghost">
          <a
            href="https://jazz.tools/showcase"
            target="_blank"
            rel="noopener noreferrer"
          >
            <WrenchIcon class="size-4" />
            Built with Jazz
          </a>
        </Button>
      </footer>
    </div>
  );
}
 -->
