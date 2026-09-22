/** Fixed repetition, not retries: every fresh process must pass, and any stop,
 * upstream shutdown, or receipt failure immediately terminates the proof. */
export async function verifyAndroidOfflineRestarts({ stopApp, stopUpstream, verify }) {
  for (let iteration = 1; iteration <= 3; iteration++) {
    await stopApp();
    if (iteration === 1) await stopUpstream();
    await verify(iteration);
  }
}
