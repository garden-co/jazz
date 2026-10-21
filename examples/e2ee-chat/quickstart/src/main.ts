import { account } from "./client.js";

document.querySelector<HTMLDivElement>("#app")!.textContent = `Your account ID: ${account.id}`;
