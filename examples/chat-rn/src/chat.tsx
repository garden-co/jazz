import Clipboard from "@react-native-clipboard/clipboard";
import {
  CoPlainText,
  getLoadedOrUndefined,
  Group,
  ID,
  LastAndAllCoMapEdits,
  Loaded,
} from "jazz-tools";
import { useAccount, useCoState, useLogOut } from "jazz-tools/react-native";
import { useEffect, useState } from "react";
import {
  Button,
  FlatList,
  KeyboardAvoidingView,
  SafeAreaView,
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
} from "react-native";
import { Chat, Message } from "./schema";

export function ChatScreen({ navigation }: { navigation: any }) {
  const me = useAccount();
  const logOut = useLogOut();
  const [chatId, setChatId] = useState<string>();
  const [chatIdInput, setChatIdInput] = useState<string>();
  const loadedChat = useCoState(Chat, chatId, {
    resolve: { $each: { text: true } },
  });
  const [message, setMessage] = useState("");
  const profile = getLoadedOrUndefined(me)?.profile;

  function handleLogOut() {
    setChatId(undefined);
    logOut();
  }

  useEffect(() => {
    navigation.setOptions({
      headerRight: () => <Button onPress={handleLogOut} title="Logout" />,
      headerLeft: () =>
        loadedChat ? (
          <Button
            onPress={() => {
              if (loadedChat?.$jazz.id) {
                Clipboard.setString(loadedChat.$jazz.id);
                console.log(
                  "Copied to clipboard",
                  `Chat ID: ${loadedChat.$jazz.id}`,
                );
              }
            }}
            title="Share"
          />
        ) : null,
    });
  }, [navigation, loadedChat]);

  const createChat = () => {
    if (!me.$isLoaded) return;
    const group = Group.create({ owner: me });
    group.addMember("everyone", "writer");
    const chat = Chat.create([], { owner: group });
    setChatId(chat.$jazz.id);
  };

  const joinChat = () => {
    if (chatIdInput) {
      setChatId(chatIdInput as ID<typeof Chat>);
    } else {
      console.warn("Error: Chat ID cannot be empty.");
    }
  };

  const sendMessage = () => {
    if (!loadedChat.$isLoaded) return;
    if (message.trim()) {
      loadedChat.$jazz.push(
        Message.create(
          { text: CoPlainText.create(message, loadedChat.$jazz.owner) },
          loadedChat.$jazz.owner,
        ),
      );
      setMessage("");
    }
  };

  const renderMessageItem = ({
    item,
  }: {
    item: Loaded<typeof Message, { text: true }>;
  }) => {
    const isMe = item.$jazz.getEdits()?.text?.by?.isMe;
    return (
      <View
        style={[
          styles.messageContainer,
          isMe ? styles.myMessage : styles.otherMessage,
        ]}
      >
        {!isMe ? (
          <Text
            style={[
              styles.senderName,
              isMe ? styles.textRight : styles.textLeft,
            ]}
          >
            {getEditorName(item?.$jazz.getEdits()?.text)}
          </Text>
        ) : null}
        <View style={styles.messageContent}>
          <Text style={styles.messageText}>{item.text}</Text>
          <Text
            style={[
              styles.timestamp,
              !isMe ? styles.timestampOther : styles.timestampMy,
            ]}
          >
            {item?.$jazz
              .getEdits()
              ?.text?.madeAt?.getHours()
              .toString()
              .padStart(2, "0")}
            :
            {item?.$jazz
              .getEdits()
              ?.text?.madeAt?.getMinutes()
              .toString()
              .padStart(2, "0")}
          </Text>
        </View>
      </View>
    );
  };

  return (
    <SafeAreaView style={styles.container}>
      {!loadedChat.$isLoaded ? (
        <View style={styles.welcomeContainer}>
          <Text style={styles.usernameLabel}>Username</Text>
          <TextInput
            style={styles.usernameInput}
            value={getLoadedOrUndefined(profile)?.name ?? ""}
            onChangeText={(value) => {
              if (profile?.$isLoaded) {
                profile.$jazz.set("name", value);
              }
            }}
            textAlignVertical="center"
            onSubmitEditing={sendMessage}
            testID="username-input"
          />
          <TouchableOpacity onPress={createChat} style={styles.newChatButton}>
            <Text style={styles.buttonText}>Start new chat</Text>
          </TouchableOpacity>
          <Text style={styles.usernameLabel}>Join existing chat</Text>
          <TextInput
            style={styles.chatIdInput}
            placeholder="Chat ID"
            value={chatIdInput ?? ""}
            onChangeText={setChatIdInput}
            textAlignVertical="center"
            onSubmitEditing={joinChat}
            testID="chat-id-input"
          />
          <TouchableOpacity onPress={joinChat} style={styles.joinChatButton}>
            <Text style={styles.buttonText}>Join chat</Text>
          </TouchableOpacity>
        </View>
      ) : (
        <>
          <FlatList
            contentContainerStyle={{
              flexGrow: 1,
              flex: 1,
              gap: 6,
              padding: 8,
            }}
            style={styles.messageList}
            data={loadedChat}
            keyExtractor={(item) => item.$jazz.id}
            renderItem={renderMessageItem}
          />

          <KeyboardAvoidingView
            keyboardVerticalOffset={110}
            behavior="padding"
            style={styles.inputContainer}
          >
            <View style={styles.inputWrapper}>
              <TextInput
                style={styles.messageInput}
                value={message}
                onChangeText={setMessage}
                placeholder="Type a message..."
                textAlignVertical="center"
                onSubmitEditing={sendMessage}
                testID="message-input"
              />
              <TouchableOpacity
                onPress={sendMessage}
                style={styles.sendButton}
                testID="send-button"
              >
                <Text>↑</Text>
              </TouchableOpacity>
            </View>
          </KeyboardAvoidingView>
        </>
      )}
    </SafeAreaView>
  );
}

function getEditorName(
  edit?: LastAndAllCoMapEdits<CoPlainText>,
): string | undefined {
  if (!edit?.by?.profile || !edit.by.profile.$isLoaded) {
    return;
  }
  return edit.by.profile.name;
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
  welcomeContainer: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    padding: 16,
  },
  usernameLabel: {
    fontSize: 16,
    fontWeight: "bold",
    marginBottom: 16,
  },
  usernameInput: {
    width: 160,
    height: 48,
    borderWidth: 1,
    borderColor: "#ddd",
    borderRadius: 8,
    paddingHorizontal: 8,
    marginBottom: 24,
  },
  chatIdInput: {
    width: 320,
    height: 48,
    borderWidth: 1,
    borderColor: "#ddd",
    borderRadius: 8,
    paddingHorizontal: 8,
    marginBottom: 24,
    marginTop: 8,
  },
  newChatButton: {
    backgroundColor: "#3B82F6",
    padding: 16,
    borderRadius: 8,
    marginBottom: 24,
  },
  joinChatButton: {
    backgroundColor: "#10B981",
    padding: 16,
    borderRadius: 8,
  },
  buttonText: {
    color: "white",
    fontSize: 16,
    fontWeight: "bold",
    textAlign: "center",
  },
  messageList: {
    flex: 1,
  },
  inputContainer: {
    padding: 12,
  },
  inputWrapper: {
    flexDirection: "row",
    alignItems: "center",
    borderWidth: 1,
    borderColor: "#ddd",
    borderRadius: 8,
    paddingHorizontal: 12,
  },
  messageInput: {
    flex: 1,
    height: 32,
    paddingHorizontal: 8,
  },
  sendButton: {
    paddingHorizontal: 16,
  },
  messageContainer: {
    flexDirection: "column",
    alignItems: "flex-start",
    marginBottom: 8,
  },
  myMessage: {
    alignSelf: "flex-end",
  },
  otherMessage: {
    alignSelf: "flex-start",
  },
  senderName: {
    fontSize: 12,
    color: "#666",
    marginBottom: 4,
  },
  textRight: {
    textAlign: "right",
  },
  textLeft: {
    textAlign: "left",
  },
  messageContent: {
    backgroundColor: "#f0f0f0",
    borderRadius: 8,
    padding: 8,
  },
  messageText: {
    fontSize: 16,
  },
  timestamp: {
    fontSize: 10,
    color: "#666",
    marginTop: 4,
  },
  timestampOther: {
    textAlign: "left",
    marginTop: 8,
  },
  timestampMy: {
    textAlign: "right",
    marginTop: 4,
  },
  avatar: {
    borderRadius: 16,
    height: 32,
    width: 32,
    alignItems: "center",
    justifyContent: "center",
  },
});
