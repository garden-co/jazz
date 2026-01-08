import { createInviteLink, co } from "jazz-tools";
import { Festival } from "./schema";

export const FestivalComponent = (festival: co.loaded<typeof Festival>) => {
  const festivalSection = document.createElement('section');
  const form = document.createElement('form');
  const input = Object.assign(document.createElement('input'), {
    type: 'text',
    name: 'band',
    placeholder: 'Band name'
  });
  const button = Object.assign(document.createElement('button'), {
    name: 'band',
    innerText: 'Add',
    onclick: async () => {
      festival.$jazz.push({ name: input.value });
      input.value = '';
    }
  });

  form.append(input, button);
  festivalSection.append(form);

  const inputLink = document.createElement('output');
  const createLinkButton = Object.assign(document.createElement('button'), {
    innerText: 'Create Invite Link',
    onclick: () => {
      const inviteLink = createInviteLink(
        festival,
        "writer",
        window.location.host
      );
      inputLink.value = inviteLink;
    }
  });
  festivalSection.append(inputLink, createLinkButton);


  const bandList = document.createElement('ul');
  const unsubscribe = festival.$jazz.subscribe((festival) => {
    if (!festival.$isLoaded) throw new Error("Festival not loaded");

    const bandElements = festival
      .map((band) => {
        if (!band.$isLoaded) return;
        const bandElement = document.createElement("li");
        bandElement.innerText = band.name;
        return bandElement;
      })
      .filter((band) => band !== undefined);

    bandList.replaceChildren(...bandElements);
  });

  festivalSection.append(bandList);
  return festivalSection;
}