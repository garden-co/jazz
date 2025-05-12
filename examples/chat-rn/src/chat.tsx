import Clipboard from "@react-native-clipboard/clipboard";
import { useAccount, useCoState } from "jazz-react-native";
import { CoPlainText, Group, ID, Profile } from "jazz-tools";
import { useEffect, useState } from "react";
import {
  Alert,
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
  const { me, logOut } = useAccount();
  const [chatId, setChatId] = useState<ID<Chat>>();
  const loadedChat = useCoState(Chat, chatId, { resolve: { $each: true } });
  const [message, setMessage] = useState("");
  const profile = useCoState(Profile, me._refs.profile?.id, {});

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
              if (loadedChat?.id) {
                Clipboard.setString(
                  `https://chat.jazz.tools/#/chat/${loadedChat.id}`,
                );
                Alert.alert("Copied to clipboard", `Chat ID: ${loadedChat.id}`);
              }
            }}
            title="Share"
          />
        ) : null,
    });
  }, [navigation, loadedChat]);

  const createChat = () => {
    const group = Group.create({ owner: me });
    group.addMember("everyone", "writer");
    const chat = Chat.create([], { owner: group });
    setChatId(chat.id);
  };

  const joinChat = () => {
    Alert.prompt(
      "Join Chat",
      "Enter the Chat ID (example: co_zBGEHYvRfGuT2YSBraY3njGjnde)",
      [
        {
          text: "Cancel",
          style: "cancel",
        },
        {
          text: "Join",
          onPress: (chatId) => {
            if (chatId) {
              setChatId(chatId as ID<Chat>);
            } else {
              Alert.alert("Error", "Chat ID cannot be empty.");
            }
          },
        },
      ],
      "plain-text",
    );
  };

  const sendMessage = () => {
    if (!loadedChat) return;
    if (message.trim()) {
      loadedChat.push(
        Message.create(
          { text: CoPlainText.create(message, loadedChat?._owner) },
          loadedChat?._owner,
        ),
      );
      setMessage("");
    }
  };

  const renderMessageItem = ({ item }: { item: Message }) => {
    const isMe = item._edits?.text?.by?.isMe;
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
            {item?._edits?.text?.by?.profile?.name}
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
            {item?._edits?.text?.madeAt?.getHours().toString().padStart(2, "0")}
            :
            {item?._edits?.text?.madeAt
              ?.getMinutes()
              .toString()
              .padStart(2, "0")}
          </Text>
        </View>
      </View>
    );
  };

  return (
    <SafeAreaView style={styles.container}>
      {!loadedChat ? (
        <View style={styles.welcomeContainer}>
          <Text style={styles.usernameLabel}>Username</Text>
          <TextInput
            style={styles.usernameInput}
            value={profile?.name ?? ""}
            onChangeText={(value) => {
              if (profile) {
                profile.name = value;
              }
            }}
            textAlignVertical="center"
            onSubmitEditing={sendMessage}
            testID="username-input"
          />
          <TouchableOpacity onPress={createChat} style={styles.newChatButton}>
            <Text style={styles.buttonText}>Start new chat</Text>
          </TouchableOpacity>
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
            keyExtractor={(item) => item.id}
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

const styles = StyleSheet.create({
  container: {
    flex: 1,
    flexDirection: "column",
    height: "100%",
  },
  messageContainer: {
    borderRadius: 8,
    paddingVertical: 4,
    paddingHorizontal: 6,
    maxWidth: "80%",
  },
  myMessage: {
    backgroundColor: "#e5e7eb", // gray-200
    alignSelf: "flex-end",
    textAlign: "right",
  },
  otherMessage: {
    backgroundColor: "#d1d5db", // gray-300
    alignSelf: "flex-start",
  },
  senderName: {
    fontSize: 12,
    color: "#6b7280", // gray-500
  },
  textRight: {
    textAlign: "right",
  },
  textLeft: {
    textAlign: "left",
  },
  messageContent: {
    position: "relative",
    flexDirection: "row",
    alignItems: "flex-end",
    justifyContent: "space-between",
  },
  messageText: {
    color: "#000000",
    fontSize: 16,
    maxWidth: "85%",
  },
  timestamp: {
    fontSize: 10,
    color: "#6b7280", // gray-500
    textAlign: "right",
    marginLeft: 8,
  },
  timestampOther: {
    marginTop: 8,
  },
  timestampMy: {
    marginTop: 4,
  },
  welcomeContainer: {
    flex: 1,
    flexDirection: "column",
    height: "100%",
    alignItems: "center",
    justifyContent: "center",
  },
  usernameLabel: {
    fontSize: 16,
    fontWeight: "bold",
    marginBottom: 24,
  },
  usernameInput: {
    borderRadius: 4,
    height: 48,
    padding: 8,
    marginBottom: 48,
    width: 160,
    borderWidth: 1,
    borderColor: "#e5e7eb", // gray-200
  },
  newChatButton: {
    backgroundColor: "#3b82f6", // blue-500
    padding: 16,
    borderRadius: 6,
  },
  joinChatButton: {
    backgroundColor: "#10b981", // green-500
    padding: 16,
    borderRadius: 6,
    marginTop: 16,
  },
  buttonText: {
    color: "white",
    fontWeight: "600",
  },
  messageList: {
    flex: 1,
  },
  inputContainer: {
    paddingVertical: 16,
    paddingHorizontal: 10,
    backgroundColor: "white",
    borderTopWidth: 1,
    borderTopColor: "#d1d5db", // gray-300
  },
  inputWrapper: {
    flex: 1,
    flexDirection: "row",
    alignItems: "center",
    margin: 5,
  },
  messageInput: {
    borderRadius: 20,
    height: 32,
    paddingVertical: 0,
    paddingHorizontal: 8,
    borderWidth: 1,
    borderColor: "#e5e7eb", // gray-200
    flex: 1,
  },
  sendButton: {
    marginLeft: 10,
    backgroundColor: "#d1d5db", // gray-300
    borderRadius: 16,
    height: 32,
    width: 32,
    alignItems: "center",
    justifyContent: "center",
  },
});
