import { co, z } from "jazz-tools";
const Inventory = co.record(z.string(), z.number());

// #region BracketNotation
const inventory = Inventory.create({
  tomatoes: 48,
  peppers: 24,
  basil: 12,
});

console.log(inventory["tomatoes"]); // 48
// #endregion

const Message = co.map({
  content: z.string(),
});
const MyAppUser = co.account({
  profile: co.profile({
    avatar: co.image(),
  }),
  root: co.map({}),
});
const chatId = "";
const me = await MyAppUser.load("");
if (!me.$isLoaded) throw new Error();

// #region RecordAsSet
const Chat = co.map({
  messages: co.list(Message),
  participants: co.record(z.string(), MyAppUser),
});

const chat = await Chat.load(chatId, {
  resolve: {
    participants: true,
  },
});

let participantList: string[];

// Note that I don't need to load the map deeply to read and set keys
if (chat.$isLoaded) {
  chat.participants.$jazz.set(me.$jazz.id, me);
  participantList = Object.keys(chat.participants);
}
// #endregion

if (!chat.$isLoaded) throw new Error();
// #region DeeplyLoadingKeys
const { participants } = await chat.$jazz.ensureLoaded({
  resolve: {
    participants: {
      $each: {
        profile: {
          avatar: true,
        },
      },
    },
  },
});

const avatarList = Object.values(participants).map(
  (user) => user.profile.avatar,
);
// #endregion
