// The shape of a walkthrough storyboard: what each video shows, in order.
// See README.md. Storyboards are plain data; `walkthrough.mjs` plays them.

/** A device on the stage, by the id its storyboard gives it (usually "a", "b" or "p"). */
export type DeviceId = string;

export type Device = {
  /** Shown in a laptop's menu bar. */
  name?: string;
  /** Shown in a laptop's browser toolbar. */
  address?: string;
  /** The device's cursor colour. */
  color?: string;
  /** A phone has a status bar and no browser toolbar. */
  kind?: "laptop" | "phone";
  /** The page's starting viewport, before the stage lays the device out. */
  viewport?: { width: number; height: number };
};

/** A device's box on the stage, in stage pixels, frame included. */
export type Pane = {
  id: DeviceId;
  x: number;
  y: number;
  w: number;
  h: number;
  /** The app is laid out at 1 / scale of the box. */
  scale?: number;
  fill?: boolean;
};

export type Beat =
  /**
   * A subtitle in the band under the devices; "" hides it. Then waits `hold`
   * ms. "{name}" or "{name.0}" inserts a value an action kept in `state`.
   */
  | { caption: string; hold?: number }
  /** A full-stage title card, shown for `hold` ms. */
  | { title: string; text: string; hold?: number }
  /** A named action from the walkthrough's module, on one device. */
  | { do: string; on?: DeviceId; args?: unknown[] }
  /** Pauses for `wait` ms. */
  | { wait: number }
  /** Turns a device's Wi-Fi off or on from its menu bar, outside the app. */
  | { wifi: "off" | "on"; on: DeviceId }
  /** One device fills the stage. */
  | { full: DeviceId }
  /** Two devices side by side, each laid out at 1 / scale of its box. */
  | { split: [DeviceId, DeviceId]; scale?: number }
  /** Devices at explicit boxes. */
  | { show: Pane[] }
  /** The poster frame is this moment. */
  | { poster: true }
  /** Waits until the text is visible on the device. */
  | { see: string; on: DeviceId; exact?: boolean; timeout?: number }
  /** Fails the recording if the text is on the device, for example an edit made offline. */
  | { notSee: string; on: DeviceId; because: string };

export type Storyboard = {
  /** Output name: public/examples/videos/<id>.mp4 and .jpg. */
  id: string;
  /**
   * One sentence on what the video shows. The examples page shows it under
   * the video (lib/showcase/catalogue.ts reads it from here).
   */
  summary?: string;
  /** Stage size and subtitle size; `webgl` turns on software WebGL (slower). */
  stage?: { width?: number; height?: number; captionSize?: number; webgl?: boolean };
  devices: Record<DeviceId, Device>;
  /** Before the camera rolls: sign-ups, seeding, compiling routes. */
  offCamera?: Beat[];
  /** After the stage starts, before the video begins: the opening layout. */
  opening: Beat[];
  /** The video, in order. */
  beats: Beat[];
};
