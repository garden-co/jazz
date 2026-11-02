/**
 * A short synthesized "rough mix" (a plucked seventh-chord figure) so the
 * seeded conversation has a real, playable audio attachment without shipping
 * a binary file. 16-bit mono PCM WAV.
 */
export function demoRoughMix(seconds = 8, sampleRate = 22_050): Uint8Array {
  const samples = seconds * sampleRate;
  const buffer = new ArrayBuffer(44 + samples * 2);
  const view = new DataView(buffer);
  const ascii = (offset: number, text: string) =>
    [...text].forEach((char, index) => view.setUint8(offset + index, char.charCodeAt(0)));
  ascii(0, "RIFF");
  view.setUint32(4, 36 + samples * 2, true);
  ascii(8, "WAVE");
  ascii(12, "fmt ");
  view.setUint32(16, 16, true);
  view.setUint16(20, 1, true); // PCM
  view.setUint16(22, 1, true); // mono
  view.setUint32(24, sampleRate, true);
  view.setUint32(28, sampleRate * 2, true);
  view.setUint16(32, 2, true);
  view.setUint16(34, 16, true);
  ascii(36, "data");
  view.setUint32(40, samples * 2, true);

  // Dm7 - G7 - Cmaj7 - A7, one bar each, eighth-note arpeggios.
  const chords = [
    [146.83, 174.61, 220.0, 261.63],
    [196.0, 246.94, 293.66, 349.23],
    [130.81, 164.81, 196.0, 246.94],
    [110.0, 138.59, 164.81, 196.0],
  ];
  const noteLength = seconds / (chords.length * 8);
  for (let i = 0; i < samples; i++) {
    const t = i / sampleRate;
    const step = Math.floor(t / noteLength);
    const chord = chords[Math.floor(step / 8) % chords.length]!;
    const frequency = chord[step % 4]! * (step % 8 >= 4 ? 2 : 1);
    const sinceOnset = t - step * noteLength;
    const envelope = Math.exp(-sinceOnset * 6);
    const bass = 0.25 * Math.sin(2 * Math.PI * chord[0]! * 0.5 * t);
    const value =
      0.45 *
        envelope *
        (Math.sin(2 * Math.PI * frequency * t) + 0.3 * Math.sin(4 * Math.PI * frequency * t)) +
      bass;
    view.setInt16(44 + i * 2, Math.max(-1, Math.min(1, value * 0.6)) * 0x7fff, true);
  }
  return new Uint8Array(buffer);
}
