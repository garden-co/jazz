import { MyAppAccount } from "./schema";

const unsubscribe = MyAppAccount.getMe().$jazz.subscribe(
  {
    resolve: {
      profile: true,
      root: {
        myChats: { $each: true },
      },
    },
  },
  (account) => {
    if (!account.$isLoaded) return; // Handle loading/error states
    const myNameElement = document.getElementById("my-name");
    if (myNameElement) {
      myNameElement.textContent = account.profile.name;
    }
  },
);

// When you're ready to clean up:
unsubscribe();
