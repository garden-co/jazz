/** SvelteKit virtual module boundary for the real starter admission fixture. */
export async function goto(_url: string): Promise<void> {
  throw new Error("Starter navigation must be mocked by the test");
}
