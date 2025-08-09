import { Account } from "jazz-tools";
import { createImage, useAccount, useCoState } from "jazz-tools/react";
import { useEffect, useState } from "react";
import { Chat, Message } from "./schema.ts";
import {
  BubbleBody,
  BubbleContainer,
  BubbleImage,
  BubbleInfo,
  BubbleText,
  ChatBody,
  EmptyChatMessage,
  ImageInput,
  InputBar,
  TextInput,
} from "./ui.tsx";

const INITIAL_MESSAGES_TO_SHOW = 30;

export function ChatScreen(props: { chatID: string }) {
  const chat = useCoState(Chat, props.chatID);
  const { me } = useAccount();
  const [showNLastMessages, setShowNLastMessages] = useState(
    INITIAL_MESSAGES_TO_SHOW,
  );
  const isLoading = useMessagesPreload(props.chatID);

  if (!chat || isLoading)
    return (
      <div className="flex-1 flex justify-center items-center">Loading...</div>
    );

  const sendImage = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.currentTarget.files?.[0];

    if (!file) return;

    if (file.size > 5000000) {
      alert("Please upload an image less than 5MB.");
      return;
    }

    createImage(file, { owner: chat._owner }).then((image) => {
      chat.push(
        Message.create(
          {
            text: file.name,
            image: image,
          },
          chat._owner,
        ),
      );
    });
  };

  if (!me) {
    return <div>Loading...</div>;
  }

  return (
    <>
      <ChatBody>
        {chat.length > 0 ? (
          chat
            // We call slice before reverse to avoid mutating the original array
            .slice(-showNLastMessages)
            // Reverse plus flex-col-reverse on ChatBody gives us scroll-to-bottom behavior
            .reverse()
            .map(
              (msg) =>
                msg?.text && <ChatBubble me={me} msg={msg} key={msg.id} />,
            )
        ) : (
          <EmptyChatMessage />
        )}
        {chat.length > showNLastMessages && (
          <button
            className="px-4 py-1 block mx-auto my-2 border rounded"
            onClick={() => setShowNLastMessages(showNLastMessages + 10)}
          >
            Show more
          </button>
        )}
      </ChatBody>

      <InputBar>
        <ImageInput onImageChange={sendImage} />

        <TextInput
          onSubmit={(text) => {
            chat.push(Message.create({ text }, chat._owner));
          }}
        />
      </InputBar>
    </>
  );
}

function ChatBubble(props: {
  me: Account;
  msg: Message;
}) {
  if (!props.me.canRead(props.msg) || !props.msg.text?.toString()) {
    return (
      <BubbleContainer fromMe={false}>
        <BubbleBody fromMe={false}>
          <BubbleText
            text="Message not readable"
            className="text-gray-500 italic"
          />
        </BubbleBody>
      </BubbleContainer>
    );
  }

  const lastEdit = props.msg._edits.text;
  const fromMe = lastEdit?.by?.isMe;
  const { text, image } = props.msg;

  return (
    <BubbleContainer fromMe={fromMe}>
      <BubbleBody fromMe={fromMe}>
        {image && <BubbleImage image={image} />}
        <BubbleText text={text} />
      </BubbleBody>
      {lastEdit && (
        <BubbleInfo by={lastEdit.by?.profile?.name} madeAt={lastEdit.madeAt} />
      )}
    </BubbleContainer>
  );
}

/**
 * Warms the local cache with the initial messages to load only the initial messages
 * and avoid flickering
 */
function useMessagesPreload(chatID: string) {
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    preloadChatMessages(chatID).finally(() => {
      setIsLoading(false);
    });
  }, [chatID]);

  return isLoading;
}

async function preloadChatMessages(chatID: string) {
  const chat = await Chat.load(chatID);

  if (!chat?._refs) return;

  const promises = [];

  for (const msg of Array.from(chat._refs)
    .reverse()
    .slice(0, INITIAL_MESSAGES_TO_SHOW)) {
    promises.push(Message.load(msg.id, { resolve: { text: true } }));
  }

  await Promise.all(promises);
}
