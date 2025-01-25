import Clipboard from "@react-native-clipboard/clipboard";
import { createInviteLink } from "jazz-react-native";
import { useCoState } from "jazz-react-native";
import { Group, ID } from "jazz-tools";
import React from "react";
import { useState } from "react";
import { Alert, Button, StyleSheet, Text, View } from "react-native";
import { CoMapWithText } from "../schema";

export function SimpleSharing() {
  const [id, setId] = useState<ID<CoMapWithText> | undefined>();
  const [invite, setInvite] = useState<string | undefined>();
  const coMap = useCoState(CoMapWithText, id);

  function handleCreateCoMap() {
    const group = Group.create();

    const lCoMap = CoMapWithText.create(
      { text: "Updated from React Native" },
      { owner: group },
    );

    const lInvite = createInviteLink(lCoMap, "writer")
      .replace("undefined/", "/")
      .replace("#", "")
      .replace("http://", "https://");

    setId(lCoMap.id);

    const validateCmd = `node validateCoValue.mjs ${lInvite}`;
    console.log(validateCmd);
    Clipboard.setString(validateCmd);
    Alert.alert("Validate command copied to clipboard");

    setInvite(lInvite);
  }

  return (
    <>
      <Button onPress={handleCreateCoMap} title="Create CoMap" />
      <Text id="coMapText" style={styles.coMapText}>
        {coMap?.text}
      </Text>
      {invite && (
        <View style={styles.container}>
          <Text style={styles.inviteCode}>Invite code</Text>
          <Text selectable id="invite">
            {invite}
          </Text>
        </View>
      )}
    </>
  );
}

const styles = StyleSheet.create({
  container: {
    display: "flex",
    flexDirection: "column",
    gap: 10,
    padding: 2,
    justifyContent: "center",
    alignItems: "center",
  },
  coMapText: {
    fontSize: 20,
    padding: 20,
    textAlign: "center",
  },
  inviteCode: {
    fontSize: 16,
    fontWeight: "bold",
  },
});
