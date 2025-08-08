import { useAcceptInviteNative } from "jazz-tools/react-native";
import React from "react";
import { Text } from "react-native";
import { Chat } from "./schema";

export function HandleInviteScreen({ navigation }: { navigation: any }) {
  useAcceptInviteNative({
    invitedObjectSchema: Chat,
    onAccept: async (chatId) => {
      navigation.navigate("ChatScreen", { chatId });
    },
  });

  return <Text>Accepting invite...</Text>;
}
