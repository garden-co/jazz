# create-jazz

Print the [Jazz](https://jazz.tools) agent brief: the instructions your coding agent follows to set Jazz up in your project.

```bash
npm create jazz@latest
```

The command fetches https://jazz.tools/agent-brief.md and prints it. It asks no questions and writes no files. Paste the output into your coding agent, or tell the agent to run `npm create jazz` and follow what it prints.

The brief is the same text as the [Quickstart](https://jazz.tools/docs/setup/run-locally). It lives only on the docs site, so an older `create-jazz` never prints stale instructions. When the fetch fails, the command exits non-zero and names the URL, so the agent can fetch it directly.

Arguments are ignored. Set `JAZZ_AGENT_BRIEF_URL` to print a brief from somewhere else, such as a docs preview.
