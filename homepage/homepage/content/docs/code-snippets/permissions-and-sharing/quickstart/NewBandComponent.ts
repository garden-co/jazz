import { co } from "jazz-tools";
import { Festival } from "./schema";

export const NewBandComponent = (festival: co.loaded<typeof Festival>) => {
  const form = document.createElement("form");
  const input = Object.assign(document.createElement("input"), {
    type: "text",
    name: "band",
    placeholder: "Band name",
  });
  const button = Object.assign(document.createElement("button"), {
    name: "band",
    innerText: "Add",
    onclick: async (e: Event) => {
      e.preventDefault();
      festival.$jazz.push({ name: input.value });
      input.value = "";
    },
  });

  form.append(input, button);
  return form;
};
