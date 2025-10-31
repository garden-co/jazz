import { beforeEach, expect, test } from "vitest";
import {
  loadCoValueOrFail,
  setupTestAccount,
  setupTestNode,
  SyncMessagesLog,
} from "./testUtils.js";

let jazzCloud: ReturnType<typeof setupTestNode>;

beforeEach(async () => {
  jazzCloud = setupTestNode({ isSyncServer: true });
});

test("should track the group dependency when creating a new coValue", async () => {
  const client = await setupTestAccount({
    connected: true,
  });

  const group = client.node.createGroup();
  const map = group.createMap();

  const batch1 = [
    {
      changes:
        '[{"key":"co_zFdU3Ftg3FVE7YC3vEJFbhxwYZa","op":"set","value":"admin"}]',
      madeAt: 1759431644166,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zFdU3Ftg3FVE7YC3vEJFbhxwYZa","op":"set","value":"sealed_ULeCOM4ERAZADx_ncnShF1xzIUD0cs-5B03Z1AlKTxFXImqUjphzwhG0sZYH7RsEzmbclr8cqugzRUSK8SZl1QrgShyEMOJkhyQ=="}]',
      madeAt: 1759431644182,
      privacy: "trusting",
    },
    {
      changes: '[{"key":"readKey","op":"set","value":"key_z3VotF1Cs2LW7rz7E"}]',
      madeAt: 1759431644187,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"parent_co_zFdU3Ftg3FVE7YC3vEJFbhxwYZa","op":"set","value":"extend"}]',
      madeAt: 1759431644198,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_key_z51ErAUxbYsyFyViv1","op":"set","value":"encrypted_UWILFloc5ov53e2_6tas56BTvDnf2eVuXacQQGLEftKapbz3_lltMy4yT_bArgi3B-IAy7nHJeFmU"}]',
      madeAt: 1759431644237,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1759431645206,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U41iRep7nLnrcwbSO7PVcgErnKYw5x0iR3PxKNS6V1JcDdowhqPK1tmA6ryCW50JN_hGxs920bal3ZGl_KDTjjnpzSdDTaEK5UA=="}]',
      madeAt: 1759431645222,
      privacy: "trusting",
    },
  ];

  for (const tx of batch1) {
    map.core.makeTransaction(
      JSON.parse(tx.changes),
      "trusting",
      undefined,
      tx.madeAt,
    );
  }

  await map.core.waitForSync();

  const batch2 = [
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1759431645613,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UuvwQZ1Hk8m3UfMwLPNqNmBlo1CypHVRfkY1eS9QjkOm4gUyTgF1me2UOdBnwF9QBoqyoRSSfHZWkITP1oDK8DtrrMQlhaJDDSA=="}]',
      madeAt: 1759431645627,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1759431862181,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_URokwEmQdvrFhpzn4fP0IlYEvtzlD-kQY1lHDryik1fM73Mg2WN4OYum0-x4Wrpz-Si3atsOC2Hn0AnGuUV47AgChbEpkEjTkpQ=="}]',
      madeAt: 1759431862194,
      privacy: "trusting",
    },
  ];

  const session1 = await client.spawnNewSession();
  const update1 = await loadCoValueOrFail(session1.node, map.id);

  console.log("batch2");
  SyncMessagesLog.clear();
  for (const tx of batch2) {
    update1.core.makeTransaction(
      JSON.parse(tx.changes),
      "trusting",
      undefined,
      tx.madeAt,
    );
  }

  await new Promise((resolve) => setTimeout(resolve, 1000));
  SyncMessagesLog.debugMessages({
    Group: group.core,
    Map: map.core,
  });
  await update1.core.waitForSync();

  const batch3 = [
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1760853265839,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UYZq0hzB7X8T8o_Fsm6Gx0awmucxIp2dqK5AYJH2b_xAwXOhEcNtMsXzMHKb0l-7uaCgh23hHuoidLUIX2i-U5V53KT3ZyqteUw=="}]',
      madeAt: 1760853265845,
      privacy: "trusting",
    },
  ];

  const session2 = await client.spawnNewSession();
  const update2 = await loadCoValueOrFail(session2.node, map.id);

  for (const tx of batch3) {
    update2.core.makeTransaction(
      JSON.parse(tx.changes),
      "trusting",
      undefined,
      tx.madeAt,
    );
  }

  await new Promise((resolve) => setTimeout(resolve, 1000));

  await update2.core.waitForSync();

  SyncMessagesLog.clear();
  const batch4 = [
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1760853265809,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UHLXxmvI1qTykyo6wLzTxC8qXK1IfMZpfp4i1uRbCjXbJTUXCYf3FRIXPLpXxbim58VUKMCGxHXr9eRDfrV6ZE9KFAdAqBm0drQ=="}]',
      madeAt: 1760853265813,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761049489277,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UzzwWRv6IuoYFuIeRTgNRLPlaO9xpizLrwe-k1rW9HmfiWfXBnAv4yZdCX0RKXr8EIR2TibIsPIBofHB1c3w2yb-AjWGsINuvzQ=="}]',
      madeAt: 1761049489283,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761049489809,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UIzJFMHMKLYzkjWIrDo-49MZGkHcccDQE1As-4S2kzl_nDdPvuEKjSTwdSQ31EcqK0OxKv90DypLOZqkaoPWZ_IWQtQN7y8ndcw=="}]',
      madeAt: 1761049489815,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761068717583,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UPWDurt7ZrWwMC_WcmEOUIG-q8hiqXNUoTghvLCUCqRw8k9z2eKkcPbIwAfGlnILDWhP8U4YFhfxY5naeP__-QxoFafqYQLzQlQ=="}]',
      madeAt: 1761068717589,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761068718112,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UDomvJyQSxctp50UfvDcDhCrk_GQsjsWNulckzqjJD9kfT16PvbZDYPOhlYOdsl46MwT6vAjq1jtY7AXa9rVHybnBPExLpWHLfw=="}]',
      madeAt: 1761068718117,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761068734719,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UsiQSFt8YI1sCFF0-FzPZFCugzxrVBBvLjz-wulAiq2AjQy8v5xwrN6XqW7Rqyqb6C3d-kZwrqh_viqCnRFHHJB0vgN43lSaaug=="}]',
      madeAt: 1761068734724,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761068735452,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UV_Tx0nHbVVJwZHTwYVbSdfk7kwD2yWf-utGw9-A5htA52c0tfwireIY38xqX6LivdbSM3vkRYkpiADuDPGNnSB2ufvlZcsfilg=="}]',
      madeAt: 1761068735457,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761068737762,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UcpbT7qJrm0WzS-oqnQ1XINxGfwXkRcSbX318S9HbSEiKU_b1QwxgXE-EJLbhWr9Q5YPmBr7Wkn59_TF71ANQpp4NhpdEagYERQ=="}]',
      madeAt: 1761068737768,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761068738593,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_ULYplWAkte9YumHIuUOGOYYu5wpoXx0pGKKCYhliIUaHE7WL_Ullq2D4Wf0QlR5ZxmCIMDlIq2yycHt0z-JBbLFZIyhl26CHmsw=="}]',
      madeAt: 1761068738599,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761072267356,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UjWRt60kErqeManktv_2mpsseEWNuNDhfgBUbDBOu5WBOun3SQFZZn8BxwSE1KNPpZk6WUTLRi-wUqvvZSXSTtK_iTrAVTb9KWg=="}]',
      madeAt: 1761072267362,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761072267921,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UcP3psuGgQweAMIRixHKmAT09w7gESrwTn6OHzgpB30znTurVsV9i9olEWtNnZWbOGW7cSg5sGIXsEvKAfPV03NAxWptkhBAmiQ=="}]',
      madeAt: 1761072267927,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761072572629,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UNEYpcm4ZgAvPA4_UQ62rRteE6JLDiHYuK-LrXdhU4DcfaFlZYgbHYL96KrsL1_bgd8wnS4YFVfoTh8rukU_FBLUxi3cP0oMXGw=="}]',
      madeAt: 1761072572636,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761072573189,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_US_Y2BgxnpE-0acc4ZkY8RZ60if_Zn2dDnGJxyJAjVK79fAkKVofUhfI7a7K-XUhnOPckK_Rulb5ZldyjXDMvoj7TJ8orATBIvQ=="}]',
      madeAt: 1761072573195,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761072798194,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U2gvo2xHII_TJYQB4Y2pQ66P3FUs_XCtpBWK6HSDFbrNoLUzXGQGzndlw6C9appb3tvnAMQNn0cgS9RCSq4na0jGmNtNjofdu3Q=="}]',
      madeAt: 1761072798200,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761072798763,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UZTiapAodVinEP6T-aV7YFqcUFub9a6vAidttsopMsAGXMLkah3MAX6SYZ9u4vrWr8CmqLk3HsO1Mp_88I1CCdV5koKsHVhd5Zw=="}]',
      madeAt: 1761072798768,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761072872274,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UgZW0YCeD6q_aCVV8ZLY5PqwymDf7FQFMEQcl4OLGqALwBXL7eSDRjX0L9ygWtsuymYW2Xp2wpspl-w8FFiChaBuNzU2zFfBscQ=="}]',
      madeAt: 1761072872280,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761072872843,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UOXFh1w4IsQH3XA14CKc-An4Clc1QmbwrfoBDrJ2eT6k3ti1xKrmI_eYyxBnXgk1xlg43CF40HgAJ2Xy5KSZjtI0Ibfh0cVvh3g=="}]',
      madeAt: 1761072872849,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761073028254,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UNmbIYnt1YOKUaS3ZFNmWVPDhZKnkMlV06muBWQvX45F26tDJr7Pkwqgb9Aw9nI7llj2taOgHBeJPdq68Mj3n2XpV_olMp_5DeA=="}]',
      madeAt: 1761073028260,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761073028817,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Uy2yQ149REkiLMuQf0YEECoRiJEFXkttC1y3UUpNlkPTNAxNnlqY3baqs-vmDYLGyFqMgBfuxVoVn1n2bSW47afUTJGN9tvh-LQ=="}]',
      madeAt: 1761073028824,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761073114551,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UTKZNcStAxPUkb64ITje5uip3LPmhDCivE4FvjZWPZbfb0twhHuceRj9XmE_fi9vt1Xyz4WtCkPC5BuSabAFdnZKCN6YdY6ioWA=="}]',
      madeAt: 1761073114556,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761073115120,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UPD4BO7HGGBW1rrS1IfV8GdQoI-0Fkp3Tm_sTR0kP5OR6CQx0KTfNWlPp41PRiPSmZApZmHwra-WF44ctpkxosK_7VxHkLuiOtw=="}]',
      madeAt: 1761073115126,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761074978291,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UPdGBNvIBf1fXhzCoPo10nHHpNA8EdeCddRQ61xdyaJ5oBTzzxwyZ_uIxoz2Un_FwpxcWjvmS5mcZoqjFIUZCmHfP-lNiMzHmkA=="}]',
      madeAt: 1761074978297,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761074978818,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UEXqazcINymNO8oPUSxZ87RtlAu_SSUe6pmbH45Ywb0z-mLR2SqJ0DFflGn_gU_wD4iPqjHeqxRwUwNVnI_P0ZJcqBRIiTqMbog=="}]',
      madeAt: 1761074978823,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761133524998,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Uwtyw9xI17x87u-bpjwoVpI2YYZIKw-viaRODUDgRM0oZsQtd5ytbsrVGWQhLxOZUtsA1f5CO_ghxbprrhtpKX23wcwa-bzm9JQ=="}]',
      madeAt: 1761133525004,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761133525544,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UJWxian-9BgaVizu7MtUR831MHqZgIlKKlJk5COc9TS1JpSAQ6pU7ce_5U9qhPNMOO-owige28ZLB2VCJM7F_Ty5RDMus-hnztg=="}]',
      madeAt: 1761133525550,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761133581609,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UOBPD3VKcJHiMfTfubO9J1RvOtR80UEhiC3xcOmvvgqzijB7vOCKb-RE4P22v5JLNWgCGf3WWCXjZw82PqWZM6YpvqCmUu_AVwQ=="}]',
      madeAt: 1761133581615,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761133582156,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UR1q4f7BIq2svFmgGwct3LpsE_MEbbMH7hZGb9t1yT4YgVIk2wVkYnzaIiQUwzfdB71j_qfGtTRWfDz1nVKevTIcRUDIkrzJYgw=="}]',
      madeAt: 1761133582161,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761133718481,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UuQzeA69IlATm04MVAcysQLPHtqfZGUEFswMFw0C0wL40eQ8-Pm9eEmmY9DjWe0GMTB_cKQLQPpESKWkGyfC63kwN71sFa1HhJw=="}]',
      madeAt: 1761133718486,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761133719005,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Uf1bmolIaaJAO4_6ZwvZiZjEOaT6RxcHEdFBfQBhp9c1BuRTxojpHAEg6HBOez9RzUXb94Tg-m_Fbr6LPSVoeAxACAWkHpunKCw=="}]',
      madeAt: 1761133719011,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761133768404,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U1LygWPOMHnSx_-lqLKwAHczwYu7g8lZLbrSBVC25mhXt2GBhIS-kVNSb43r67UB80W29O1ERkJin09dfAFpN8eJ2GFZoq9FMVQ=="}]',
      madeAt: 1761133768409,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761133768945,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UpSNg6JoytrXDYN0WKiNihxPUNY_JzgyplP1M99JbHvD54fx1dttUDbdBbK5kiODgDZNzZxv3AsxxcfGHixZQ0L6PFqh3YzW5vQ=="}]',
      madeAt: 1761133768950,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761143326000,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U8ErwsbFzTs-f28gkKdyq2cmOgSey9nRAojhiolEhfIsjPzUgJKX1VznwJcMaclu7ZqliYmDDr7GreZjrwOsRnX3Di7iPXHsOag=="}]',
      madeAt: 1761143326006,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761143326538,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U4KIgTeObqJTiyu6g7fitXd5N95oY8ZDRlIce3Z3uMfXPWlLpob1UxZMZaBtIqH80mtHKoS1kAxo9tQTMj_kSTfETcKR0B7e7aQ=="}]',
      madeAt: 1761143326544,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761143338491,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UeMqzqWd_XpS-hK_9KSf4pBvf7JF2p-i6XWN5V29ZEHs8_wRSGscvp0WQ6_mVuQ1abavfHiZ9UmbzXAa4nnTmKArFvKeBGDPcWw=="}]',
      madeAt: 1761143338496,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761143339013,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Ur8mOqy64cSc1md4Il_wQym1zH1yCzfPqFfQX19ZyeTf-8wFMNBt_X_PapZzAAicEweDhQKI0hL2PimVxkCsZF24BQwtlToMNDA=="}]',
      madeAt: 1761143339019,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761149242936,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U0ZrDqK6L7OVErhGCK54nl-Esv9_Z9fZ9r5gceuhpKHwokv204OcW2-6_AKEs3MeNBHNhL9wamtd4-M2GPH0pcYPepfHPTBq4Gw=="}]',
      madeAt: 1761149242941,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761149243499,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UILyHsSoiBGDRd3NTeFZHx3hqR5-KC6_VgtgzzOmVOWx1VRzyimxBiLhxD2SjzFLKLT-vsIM-sj2yUAg4VMsfwvqWiWEl9g246w=="}]',
      madeAt: 1761149243505,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761150419376,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UP8DynJ-bxM_M2f5xQPdbz5y0yI1m9ifPRiHQkMuejQy-He-Wcb_D_sWXnm4jXjLAufbvC-HX6DtZdr-Rdg7Ybc_e3jBrrJRLeQ=="}]',
      madeAt: 1761150419382,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761150419998,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Ut-ZlPeSN2FOip2sQYzwy5rZOqOKuk2vQC8gs6owgkM0ji-yNTpVg7eJ3EssobKODZVTM6E3SfsBQdwimpiVjY93i1t7L1p27xg=="}]',
      madeAt: 1761150420004,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761150980847,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U3QrPiULcQvUzwbutgUI6AcGnHEfman4W--O_GMIuVBKGxxRNcIMlNRuW9oKhKPitBJBRe8QC_C-ESpkiOOPp6D64CIygQW3tmQ=="}]',
      madeAt: 1761150980853,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761150981517,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UAzKsxMgdjERV7zZvnjcyDsUzFP2flyBVR-hJSOcWMIQaUkrQaQzW3pdUTx4BYX7-zK6wbuGrzlrVA4BvbWBJr1d9dDOp6j4Y4A=="}]',
      madeAt: 1761150981524,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761151109482,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UZVMTc5pL7194DSm3KLxlGvtkVbECs8pZ7DddE75XS08hP8l7FdfYD6ECNSL1OY7ERe2MYE588fS7zasm8D6DvNO7N05EqyYhuw=="}]',
      madeAt: 1761151109488,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761151110141,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UrxO6zWnP6ktsy_eWmoY5ChRoGTMuamzkX23uLX4biYG4LBcl5ioq2XqyNpY7fn_dQnlOEnwvcXYZlPlXLNlKifkvO8coUctDKQ=="}]',
      madeAt: 1761151110146,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761151171967,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U0QuuxJ7EfY2_X-oywfjbahheixPxrlN-jMgWgW_UPtFwLZoYgPb5iE22UbIesnTGyJxLcGZnniuj4IEpXpsQhzLrQeifqs3oDg=="}]',
      madeAt: 1761151171972,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761151172553,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UQgCRzGqqAfhf-wlSt9jL2RVSP8db5HzTCE3r4arxy4CTuW3Ugx1ZCiR_pXn-6x3Xfb_hvEwajrsxvbIIfzQI0ALXURzNEven_w=="}]',
      madeAt: 1761151172559,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761156828356,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U6QLyom1Tbbk2pICOqCtqAzlK2pvEj7Z-DqK1bX_8LQ9V3a-brRoXSlGxUll50J1IzzbWG84G8hdq7NXjSgvijnkxV7n_LmMUtA=="}]',
      madeAt: 1761156828361,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761156828880,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U9VHqIXCFnp8tgFVD-Vko8Tpt8_JSpb_eqjgY66czmtvccjchjgJrSe8CnCKsRUPdX1lGSFVmXBrnP3k0K-Ybm9gaYGrJRtyZSQ=="}]',
      madeAt: 1761156828885,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761157270194,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UugpU3NBPigOH9HFWIdRp2jpZWT8bf2y5hzy_SlDehd_4fivhz7aRc5fpCfiK56oxkOFxg-ZmUCy9IIDHX864zRpqWoMT-eTlPg=="}]',
      madeAt: 1761157270200,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761157270764,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UX5E4GL0ugZlAqgoM879jQfBYnRZkCyeKG7tTOXNV070_gnXGBpxJFsDF8ho9dNvnv4V8Pm200XxXG8TlRWA-PNHz5c_hkeG5mw=="}]',
      madeAt: 1761157270770,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761157323392,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UKd_-gqKXqDGmIX-q9UM7dzbATWfiiAYuj8sBTq5dbumWvcEQPHWJF4Rast6Vnu9Va1ejTm1-sUQxlVrHEZ7uOBP1P1Tx-do5xw=="}]',
      madeAt: 1761157323398,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761157323949,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UIaVvFZt6yabySh2s0HtmsUUfhwVxU4wRn4WScEboskWN_k_INaipbhDwb7cuXdoI3TqorF0LBi7SUnuqlmgky45bV6NVcc1WpQ=="}]',
      madeAt: 1761157323955,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761158747315,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Ul3__Jq3T8YRZX-6Rqnk20hMLT-UOmQLaWBAtBq1DzOS0IyESmzf0z4nh_w5tfHIkHp3cTUbLiZZTt-RFaP0e03Cz96nTpJrl8A=="}]',
      madeAt: 1761158747321,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761158747894,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UxBmgVqodONPcwYSNswOsAdzC0n-WFLqilaTs4tuT5oEGO6oz8W1kLH7DgtQUS4cABQKlLVU1VUWwqHNC3YSLi7sTncsSzzEkcQ=="}]',
      madeAt: 1761158747900,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761158892919,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Up57uJcuUUq4B1S4Fhs-i--u-VyWkPOwZlXe0nHgOTygK-QsiX0ODW-lhgdiSfW0KCbCTvOwCwHcoTTmOoch8AHTwjtcNIbP3Kg=="}]',
      madeAt: 1761158892925,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761158893474,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UtsPOPj6nqvX9LGmy10ysEMqBAVCr9kFMXvRsk3sC7L5ZaTdEcNTzjJUgMkQTUYhyWAZv7suB_ZeVPquOm9YB_dwA1zU3-DXuaQ=="}]',
      madeAt: 1761158893481,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761158938898,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UNtd09YCDKzGOGZe0JHn0IQHS0V3P-StCW63F3JRxk73WXWPSUXZFmYdQvbCQ3EgqDUel38pv5knEFsbJgc1Hy9y5POBmL3LnrQ=="}]',
      madeAt: 1761158938903,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761158939511,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U3EA3OJCtK3NM7aYxpT5cmUi436HWyzmw9Utu9VnAmC0QAcYoz18Y855ZOZdln6yQTSRA1s8YRwVldgkXifeGwuV7Nv1ratktdg=="}]',
      madeAt: 1761158939517,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761159183978,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UQ2o61R0WL40PdfcuazsiT1fXt3gTw7eob9rzxXjON8lqEQpCBVqwnQe9UWmBhYeltw620zKR8wVYpg8NPE3KMtLEVdNoOHny6Q=="}]',
      madeAt: 1761159183983,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761159184515,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Uu-VpX8L8khtBy2cJoiPwwVnj_jteD1u6z_Dhq0B1q26J9wrV_wJ_lfhKr0CmXL_B6RerC0MvNtqauOqEpmT_fC15tdlVF6VtrA=="}]',
      madeAt: 1761159184521,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761159505522,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U80-hZCH3dkYtKkFhL3ZFJYtMZcO5CS1vQ4XX_0gNS3lvSJaKxhFprtJx3XOMJjUEy_9TXB1VflvjPmT0MoaiYlZxBe_eolSwXQ=="}]',
      madeAt: 1761159505529,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761159506144,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UuPJ5nX4MkwOQwDZCvWBu4VdVV1bD5BtbDnUUTn2NUQ76nTHNwtxqq8lf_rA4neeu2mkulsvQwf6WpcEZz4fmj3m6h5I9QCAW9A=="}]',
      madeAt: 1761159506150,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761161648805,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U87OsCYTa01djzr1uHcBuBuXfmIJCYtCuZcEJz9oLdtvQZknCSrhg2Cf9IGyGmVvznYKu15ovcOV-qNelgv-ch1f2yr4QqCdoFw=="}]',
      madeAt: 1761161648811,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761161649497,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UWMItcS1t0osiNU-QWv1R28uwO61kScucAcf81U1CiQA7wyEHIjEpF5mvs9eBJegLmnOqiMNqwS5H-mhye7v49I8JuRHg5BCMZQ=="}]',
      madeAt: 1761161649504,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761238260042,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UPa0vlL6WacH3Ef6cLYqQ0TobZ0gIcRunEuF6umuKjIe-KdaCxzgKXwHt2aHhehPYCigVcqI7gIhc8mJne_L5oo2WiQc6Hud1aA=="}]',
      madeAt: 1761238260048,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761238260682,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UWEVxBfi6UwN66Vgbw7AKUToAzToQetIIuUbehCZmvFkof3d_O2m-Q3GKPAI4wnE1p71CeGte_oSEy43dDH4TChE3zN-WCKGKJQ=="}]',
      madeAt: 1761238260689,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761238418823,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Uzsn_yrbAuvZXXOhoUP8WfhstuETJ-S7h9YFubz0jSX25nbdtgRFfo47YZATIZDmCm2zZ8sTi6vbArY8Y0jdda_9Vv0cdcpj8wA=="}]',
      madeAt: 1761238418829,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761238419504,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UrHbhB4z90t9nnsETFDfYo0tM6kUCmsdKVfvK0WopOypw3mPNzE7xqX0WPZZ1Qb3_k1XnynaMDj65CY9kuKOihPqgWsVS2M0UEg=="}]',
      madeAt: 1761238419510,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761240106569,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UFmJfBjGLSq3i0L-tI0C2OoJ1GWPlrSF413NvsBbJb2zO5yMT_YpGbw0mOoM23-aanttFrl_oOnElQ1LdsoDuKT92XO85dbpUhg=="}]',
      madeAt: 1761240106574,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761240517350,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UCWCRsyublRjd5rSKM6w9enjtju11ABWc0BoKIbLDKzSkmlUrFSRhgG8oP5-XcEMnqhJ5gvMDbUyLiHWUuNYZjYSJdaBmJtUCLQ=="}]',
      madeAt: 1761240517357,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761240517957,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UeM4d5-w7gJ_eO2FLevTMo9iJ1B2JNJLOe_oBqE-z9RM8Mqt2ZxAVRHTbX6Dj6iD1q5VQQ31n-vc3UgbaG-Ick-21EYCOc8_-kg=="}]',
      madeAt: 1761240517964,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761240819685,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UkMHqBg-w1oxx4g43i8STiG8nOcT1PGy7iSwF6B5mi3hovWnOBAIxzIuqnivZjfnPxRJ8VloYpaHB2_x-7jihL6GQqdzsTUbdRw=="}]',
      madeAt: 1761240819692,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761240820290,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Uu1jKM2bbmU7RmpMXazAAn4VgRKTblKRYMmU7sI7grAGJc8W51_fRGeCOt0_NbceI1Bur83Xd626wyLJH55IQvAatDJygrhvBkQ=="}]',
      madeAt: 1761240820296,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761241016153,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UMuGzNpdGqUutEyWUqeOC_w63_JrF_xCupbhg4EbdVdX0gGlXE7Tk9eI60oAhVuNvyeA0uy7gT-6Fvocqxput6P2Cj1Gwr7IGfA=="}]',
      madeAt: 1761241016159,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761241016851,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U0ebiHQqrPK2kK0zeiTJk-viV04L16ITE3rfmniXgeThHITuhvE_CaohkYF3FIUUwLBWX0lvdonHmA8AbokbxTELArPlw1ZGqaQ=="}]',
      madeAt: 1761241016857,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761241030065,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UmArVd57KEP_RxJ8OEWt8wl7Z9jr2pm3XF4z_cvYYTgsh0WOc0hoUCwiTwdWz_G9H8BIFKwVxZf3YtzHg4xZkvGl--aPi_QFT9A=="}]',
      madeAt: 1761241030071,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761241030829,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_USA1QbWEvBV04gPUMXUCINmZpQNfPYxs4-l4riB5UYx26yuCQ5gU7w8wxJcFD_dpeGwrlii51V2osk4-LUXOIhdZ-K1sOnMU_iw=="}]',
      madeAt: 1761241030835,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761323183352,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UFjb4g4PE3dFS4X9HmNFEHgmBYc4PkZBJLaVaZOrPgKCX4vGpUNrkFNG0JF9h7Kf9sOu9EboAa2PBQ0zzeoWUHaZ69BOStBsd9A=="}]',
      madeAt: 1761323183358,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761323183926,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UFPkOuqFifyhr_qx0P947R10hlGuCVd6qUQLM38eMPcK96RXNrwuUuKJmkv4bAYzt11UBkoKyNxrSo6bk70EHkQNxM2Ni7uCtMQ=="}]',
      madeAt: 1761323183932,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761370714686,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UMZXjWeOdFympLX_2Lzu7LVD9ORz0plLkxUhJ0IO33Ov3WgvJyh9gLjiKCFzW74YOT9kRFlaUJRWcflXzUOeDl_dj9u08J8b98A=="}]',
      madeAt: 1761370714692,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761370715374,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UFHrB7pP8HpFjeimdJyxQz-cMm02vHOnsSGYGloZav00s1QFrhORu8cSgFyHp6rWf9Syc8MFW2ZaGyB0W0P2bHhfxIBEzXXP7Vw=="}]',
      madeAt: 1761370715380,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761371063036,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UjV3ODjYYK7HSrT9L66ZnJ88lEHJze5TcIzr6zum7O6E2W0BL4nluPAgsothyj0iWeGYpWXF1ZdwI0Xr0ohEIe6AMhyz8AWY8xA=="}]',
      madeAt: 1761371063043,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761371063663,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UpreS6O5AmHF3OtiBeh-8HqyM-YN-PKtIhPZlLGdNuWjaX8Ic5asO4a3GJuI52TRAIRMSfVid-pg7x0RMbBV9tYypVhPaMRXkSg=="}]',
      madeAt: 1761371063669,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761371152676,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Ur6NQYe241tBC9DnnWvFv3tGN5ydQ1yPKjEUTEqPNrekQV3kfwkjJqPkUGJODjx4JvtAUotUuBk8fnYLuszI93_ev7BLWc3ox4A=="}]',
      madeAt: 1761371152682,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761371153307,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UTAqrjZGrx6nJ4VS9WIbCw6fB_axMJZJB145F_LboTXcCXM_mMTCi2_bOVzA31C5NriKba3bSfOJZhzNjPx0CZkwl7WuYBkuR_A=="}]',
      madeAt: 1761371153314,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761372619534,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UznnZ5SZMJ1XXTOVRDfPXakxGBuMOziNnzHxB3hfjgF1BPSY1xgilS40TzH9Wf91PjQWJMz7taSaDV7NnwVZ6zXkvSxPFAFzlIg=="}]',
      madeAt: 1761372619540,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761372620159,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Up56D3qmiASD63hiw9ML_K5kh7fsolhDvvxUUPL2N9RM8PWnpZG4OXDJTD7RJJEIHBN7Q5bfD70Wh9L9ZfjzyKnWHZ1QG2tzOXw=="}]',
      madeAt: 1761372620166,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761373050377,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Um9Oyi-1RU0WKOYma6eyHfiBppeqNkAcPyzSDKQIOTU-3NJjtxsYmYDx_UErVeNHc6NuMftI9sVUQkWW6NZet6_EoFjvlu6nntw=="}]',
      madeAt: 1761373050384,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761373051062,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UG67CsAf6U2TxVS-yCQBNjiMzdSAyxe8Q3472TIS9qXbaLdtaba171ulJ5WYSUulUPLBJGl0wHPXEIe6vujo2ZrPm2Avm4gBmZg=="}]',
      madeAt: 1761373051068,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761373136742,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Uc4TxxuNvAItunJFAOQqzvT0WaCmTtoqFpncN0FYTZy68tWHYBdAGyg4uTX-9eQvUGKyKJV4E-J2YGFY2jOf4qegT9n8YFWogAg=="}]',
      madeAt: 1761373136749,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761373137375,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Uu87bie28ZX-sifV6jqM2U9RgrieHsPrR6_3ANY2aGPD45CyFJksIagnJfzQMNnD_T-x6vLcSTm_WdrlPCj80S_Ao7naWF0utDw=="}]',
      madeAt: 1761373137382,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761373299835,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UL2Y7oGT8jYw1BUBeH6I37_ye43Pu1Je2dryynlfDxRWNJ4JQ02IhLya8Y32c7pg2fAv0UNYpX0WUFto76qXLU4jMOZvES_7BzA=="}]',
      madeAt: 1761373299842,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761373300473,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UCdXHTRumD7wVrdmyC1DVhgUG2cbWV8jZk8kApHPZfkFjgXV6Q6K3X1LYgnxFE3Y1dGe2mcYbMlaLDSxrI-3oYgOsmQ8ZKRobyg=="}]',
      madeAt: 1761373300479,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761373575482,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Uluv67rxEna1kOGdJCJ__68S5xUYt7dvFa5KHEug325Uw8YPVsWGVDWbGjB-9EqxPQUjI9MG0vXDiF127xTmMyrlAYXvr9KKKhQ=="}]',
      madeAt: 1761373575488,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761373576115,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UIkuE-LiLIV80fbeppbCfSnBcUG_Xf3cl4NdmJuHV149MH5s53PYaedG7cVohk0LYtPEL1XWa604rUe9HPEjYgvZlL3-biybOOg=="}]',
      madeAt: 1761373576122,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761375326207,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UjvfJnz1Pr4QZtocjh9e30F1uuSpqKbOow4TFLX5fdus-g9x5XrmZ4H3VknJhQlo-0L_Q_roeuc1cUYFyK0KOFwmHDqfawLu8qA=="}]',
      madeAt: 1761375326213,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761375326821,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U4EwjsT4llFJ1pRCrjn1QEG9EMvMhPFBxW_trBICmNJV5kZfYI63uxBUBcDOkXakeDwtfJ1sabd_qP3P9MuSeQCCkeEoY6jo8tw=="}]',
      madeAt: 1761375326828,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761408040448,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U5GE8PHwpVUXIGHaqwVVtmmiY69bzxbxLmoYGXSSYincDoldWQ6v0GNwt50xWOVq9gqZBK2GlXjM1pfSXTvenxVN47Z-GuyMCJQ=="}]',
      madeAt: 1761408040455,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761408041087,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Ujzo_JAWPnIQb3UoN5X7l3VlzIsVw_DgElijPY82VJ8MJReWem8nFX1qL0C-KTrGrdd_lB-zWQJhFMoephyyHOlJDxhmLSAcYuw=="}]',
      madeAt: 1761408041094,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761409339787,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UkP7dmLknrsa2bxPC4d6m9BOG-ohpXXNZ48TpCXXIxxSL2etAUeJB7r_Zunlo96fAs22yb3nFk3mGJ3TNs382RZktcYOMUDHjxQ=="}]',
      madeAt: 1761409339793,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761409340484,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UcMCm7Hqm2c7ArjVn5r0v-rrJTwcgNv9n2sFEfKx0QYKDGPmnc9OwEREx3ThMtMpvEF3xDPqtm5ogVMHctrk-K4NQicjvOGvDPA=="}]',
      madeAt: 1761409340491,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761458902402,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UhaknvHd0eAY2hv1vYe_6vEGE3mlZUK6DIugjscp2gZWxfMpK6qzreQalEVdZYOexJ14ZumdZJubguwJM3u2ItqqCLNN0fk3LuQ=="}]',
      madeAt: 1761458902409,
      privacy: "trusting",
    },
  ];

  const erroredSession = await client.spawnNewSession();
  const { storage } = await erroredSession.addAsyncStorage();
  const updateErrored = await loadCoValueOrFail(erroredSession.node, map.id);

  for (const tx of batch4) {
    updateErrored.core.makeTransaction(
      JSON.parse(tx.changes),
      "trusting",
      undefined,
      tx.madeAt,
    );
  }

  await updateErrored.core.waitForSync();

  erroredSession.disconnect();

  const invalidSignatureBatch = [
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761459424955,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_USoFN2HSnE4Banbnk4mpd9UGJ10DhZqTznZiUIpCwVyt5dBwBjkItUDdcDxkFz_uBdOHipwW2mGgKIQtz5k-5E4EqRegh71oBvQ=="}]',
      madeAt: 1761459424961,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761459425561,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U7AVYeoNeZg3en0_XZzrmLPIZggUoYaUrmhzXvexkoQVOS6YcQFur6I8yy6GV0NnS5_8l7SfPDbgGtolyMTmyY83vp8c-eL8YVA=="}]',
      madeAt: 1761459425568,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761460620616,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UosT7EcIx2dXI93DXASZYLfeWOyezLe8rz5Nejt46ardTuWWYfrSojSzF83sVNRsUL2RUGjalvitgbfmcazqe-82QvtgdWeI1XQ=="}]',
      madeAt: 1761460620622,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761460621206,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Ux-z1dP1VEPM22oeAhiKsvBx8jnIiKcFDbTBq-zhAKPh8_-gdWwA14LIaVXhIR9SJWVtwPFkNcGmzM-jYPNGSOsF0OUsUw6xYdw=="}]',
      madeAt: 1761460621212,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761463253577,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U2zaqBWm1pSFGNoCHyqu9kPAy_zKfSO4o1TZtb3dMP9U8EO1RSGT1WYA0OXz8cYi-nt78Fn7sR3dKz-G6-ULNy4nSX1yThWKtZQ=="}]',
      madeAt: 1761463253583,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761463254211,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UZYIn0H-vBMK7u9A-f7SR907Jdp7nw5HTIZIfLOOn00o6o8nx6jHYamB_Ww3Yx_6BXyuQgZwVIgxhi9pMrtri30myG7els7v70w=="}]',
      madeAt: 1761463254218,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761463808977,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U1CcDT160AtHxUKXW_WbauPSzx8P9rVErghN2IY0lrpR4crytz5h5AGyQKrVR6oLsZeCaLfgnYcoP-BPUsdMrYPWyiZ36hZ6aWw=="}]',
      madeAt: 1761463808984,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761463809629,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U8UmnWkBvXKSsRggXKErlo4M4XXkMA2BuJtpVOwIKRHWeuTAcqNK7JpjUf5F6LY9S7L7m9tv-N5xum4oIDPnhuFePVWTtMFseBg=="}]',
      madeAt: 1761463809637,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761548579157,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UPYT8q5XVbtnydEcdLpeXV4NDD953sMcusj-Dh1d5moEVZcABhD8HQO80egzqnZGPtuDrSMsJWOo9vazfB8-t-qlTahm-5qr2vQ=="}]',
      madeAt: 1761548579163,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761548579747,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UqrwWuST6-CbDBZSKfA3kmREZdi16HM6qcyuyTMay-clsDfiFaKEWZLfiCq3csvgm15w-vgjXm6ERa2H-zRdlY8YAiQMaMOQ0oA=="}]',
      madeAt: 1761548579753,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761559463227,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UE8gyl6-jJxvf1ojhmiwCG7NjkN5AfwBZVY4eXtsxf3-KCbiJn16rB-ID9RHnhNQyZzMkHVi8OTF40H6t8OqKLX6RCcFV_R1IgQ=="}]',
      madeAt: 1761559463233,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761559463823,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UwNhP-pkvxieEDfYiyf5pTiDC9zDur0Qifg7aqyT_N-SOVAn9gH3ScrY4jpCGFGQiSbPtfUp8xOcErb0SU9F2nSc5LdZYv2l7bg=="}]',
      madeAt: 1761559463829,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761573159163,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UCZm70G6VW9FANfGQ-CFdlKLpXA7ZTuZzMLHBJejEz6OFMiTw1B2Elek4BRdBOPohQxQ0UuhBW09qra138maGDxpHRkrS97N4ZQ=="}]',
      madeAt: 1761573159169,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761573159795,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UCxia4GjZ6t3MeWXyQ2gm2su5s816du0BU5xrbkbNay_mQ80RXR_4buIVglpqYgRs6Hn6bXdWGVPR9SfXaYSvm-PebvqqpXGOyQ=="}]',
      madeAt: 1761573159802,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761575490265,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UQ17EPdeC6iebY8EdFDOdlF_1UYyoYqT9-RlxlQLN3IIz87yAEKhBLvv2P5ylHNq7IdD_XBPRKC0Kfg9FCaJAfyNwT1w9f6GAoA=="}]',
      madeAt: 1761575490272,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761575490883,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UT9Av54eHRmfs35-t5VXpwqRnH50LrFcGqBhJZcWdqyF4aNXIqcvoJClTPzXL7vho10ZFbI1EZYpnBWjYKKOx6h2Dhj-bO64A6A=="}]',
      madeAt: 1761575490889,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761575611860,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UKRmOKiSbx0C18Q1ldceT5hatd9jee2xoGcyA32EJE27nfk9xX6BlaUmjMuCb7N8uiHdPh3ujwiMHvSka9hZVKwjFQQTkgbEr7Q=="}]',
      madeAt: 1761575611867,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761575612467,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UP7-Giu9jIGtBI-oGImA9A3b_cZndAzGoK_rpnClB7_Pxvj09EseqTM799OGCJggy2XveP3PTzZkaDZBFMU1f0frwPMy9gTLAjA=="}]',
      madeAt: 1761575612474,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761669132946,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UL6bLOP4CMLLcrjGgEds9zLVEjwPqXe7E141cklCy39s3-9QbW4Pld7kmLX9VTSuYYhjD1AME26EI0CEp9qzycBMXVT5UyWTVyg=="}]',
      madeAt: 1761669132953,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761669133598,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UPlN7GK4Mxv3HpnFS6fTTkNqkI7yeKbInKKNJsJnPo9fD0IjcJ_QM6HxQMdzSooA0A7KCzV7Ad7UokZI-SHBOHRYaAc_C4M32IA=="}]',
      madeAt: 1761669133604,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761670887282,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UCklVW6TGpfZp78yYXjoQvg7F94srtd00zFF0kHM8QPB1aP3a9BtgTsCsWlFfYzazqlsl69vpDtCrHVKTwpGvTdsfzCxs4xJ2zQ=="}]',
      madeAt: 1761670887289,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761670887920,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UwvFFjUimnj8AixpJg5W6LYUSPzbzWyg7SVM1ROd-2Dad5eBlqYEDd2m-JjCmqc0HISyGSMdhpaUAm0S7JRefEQh8_72whtZkxQ=="}]',
      madeAt: 1761670887927,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761676277337,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U1nwaLzvh7bNO1E8tWJlvcscUoj3R1YsAtETvtKVgyIfHXM0w_LaNkepH36n0Oa-w5M2_TRYyNShevKuBJspBbE1ZVSdOWr5PPg=="}]',
      madeAt: 1761676277345,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761676277994,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Ue2aU9M8b0TIqjZ_v7uBMODCqYV4V6ilLv8GPQErB00Enp5ZZ5Ztclz-riFPnjCplV3XMedO-88keBB1FdAQYzYyatJNGcJHNuA=="}]',
      madeAt: 1761676278001,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761676402515,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UMx_g9fHi5jnuRpdoyb-uuc_UtoYr3Sb8YPUPnwds8eSgwyTxLx1Sh2B8AGNNRzvk_al--gjSqdlpO_lQKV2v1nK5lo76pqIcLw=="}]',
      madeAt: 1761676402523,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761676403142,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UOM0yB416SPZbAefIeQnjrABdwLT3cI1Z8FMxGNphIOJBveES2uykNZ7Bl_Cu9phFv-Q_l4xIfHFnIKt7qlo6joQpVJWHBtBx-A=="}]',
      madeAt: 1761676403148,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761680545684,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UtzW56jnzE_ALmvoauRz2705NS7kqq_nVFn07FN80bJyXYYaTN94Pa5PdgnakBXE6eA50SgbKWLcWgRxefK1BPZjX4DR79MLy7g=="}]',
      madeAt: 1761680545692,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761680546340,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Uo1_yFnM1Yaznw4vpN2zDMdJtoh9oalKiI8cgBxFp0_PDLowANxpSlKpDSiAGyIkct6rtcN5RuovGt7BR7Y9AqVCZiTfpKxqoOw=="}]',
      madeAt: 1761680546347,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761768452658,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Ua4udEpZCTYn7EDsohjRDIgc__TXV-6irNOS_qJinOJ0Mym9BfsCPx_x17oNgomVfjtp_JGS87SB3UHHa8BewTScekRm9n8FJLw=="}]',
      madeAt: 1761768452665,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761768453301,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UPMdPyjIabvZpRsFwDtuDmr1qq2rG5znPI8k-8rh7ldUWz8B5pRwOxLDzf3LNAocRQj-b-o04aRK4QTLqGWWPmoHM8aI0xWaVuw=="}]',
      madeAt: 1761768453308,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761803001387,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UP_9v0XVArCAxFcNxTK5ly_U6U9RLn99zMPia8y1a4uF6pm-Yu9RJ7G6JFmCd3CnOnacixDDDVkqQXCJpWcl46I4wiRvzFL0CcQ=="}]',
      madeAt: 1761803001394,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761803002002,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UCmLE0RSZSy7eLLR3OoyMwCx3gjdfkmpTBZ6lpc_xuswUlVHMwNORIom5-YzaIE069O1bYuS7Wb5-WD6_47JxPutd2yfm348A3A=="}]',
      madeAt: 1761803002009,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761803179093,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_U9IC5Hinz6bZq-9fxpPu3KTlY7AmfWYjF3JQY12whyrRpFm0cN2qhhK973lJ5WZugFjjeOxueT5_BE5HzfqB_25PnB_jhFxo1jg=="}]',
      madeAt: 1761803179099,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761803179697,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UnBjExcn_dp8ShzlXnS13p6Uk3yPExf2ZTLYBsL6fvJSdM0zyqljxNq38L61SCcT6b_wD541sUYf5VxwC4Qw7TQDRHO2iVxaDeg=="}]',
      madeAt: 1761803179703,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761832402867,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Um11vo798nSLUvcjLEM3z-4Pl2RwyVv6GpMg3JChdFuJNyobdrhI6CLxEFkYxREeRkj4GRuyk8UHMJcuYK3MMk9kr-mtKeIisfQ=="}]',
      madeAt: 1761832402876,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761832403566,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UcipBsfIjN1PHDxuf12AHgKyQfTk98uPMXLHAZDDCmoQebHtoN-p8Q95wtCrQ-akoMwXfUKPzZDN7Rq_Q_8y93D9HS3HpMK-jTg=="}]',
      madeAt: 1761832403574,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761832634828,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UOBQ0FFsAe77xUeuIbzWs0qfP7zQU-OkrtBwRiBIH3Koekdn7pSyJ3gqffAnV27LzdZZqfFlY5LMv7oWNXS-qIngs11AO4gef1Q=="}]',
      madeAt: 1761832634836,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761832635466,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UTVL6c4laaoScdR0sUgO7MZuhcIJWB7hijI3Yg15TavTU9ZIDp7grIpy3Io0Yo8dOT_ExogBJjybZvT-xasjPmenrb4WRz2h7tA=="}]',
      madeAt: 1761832635473,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761833051033,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_UMiyoKESKada14Mf2b1IC__U7jI9cDDFtf7VIRI14r1uUQc-cfkGrJOHSZ4jXUyNOHInUNaRFGa4G1uPy2OlvcRVGI8IQbj3DHA=="}]',
      madeAt: 1761833051040,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761833051677,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Udd4U5M5aMTiaro37LVZVGVSXdrgDp-BcmJVIiQQpAAU-9Y5Mh7bRy87jGY9jeFet5-kLfCG16vk7_oHQzKiYXqBbh231KnKu_Q=="}]',
      madeAt: 1761833051683,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"admin"}]',
      madeAt: 1761833480729,
      privacy: "trusting",
    },
    {
      changes:
        '[{"key":"key_z3VotF1Cs2LW7rz7E_for_co_zMYFsNDsR8JUQUgRZ3KdP2dJLHh","op":"set","value":"sealed_Uy5igEvjThTp_lnVrQowRWWTk-eGfnpQmfpGuWYnYfQx1kRqZndop4Q8LuhpVVjGhxtjq7pw8rj3_FfRlUhWS32FBquHr2Rshtw=="}]',
      madeAt: 1761833480736,
      privacy: "trusting",
    },
  ];

  for (const tx of invalidSignatureBatch) {
    updateErrored.core.makeTransaction(
      JSON.parse(tx.changes),
      "trusting",
      undefined,
      tx.madeAt,
    );
  }

  await updateErrored.core.waitForSync();

  const storageSession = await erroredSession.spawnNewSession();
  storageSession.addStorage({
    storage: storage,
  });

  const storageMap = await loadCoValueOrFail(storageSession.node, map.id);

  await storageMap.core.waitForSync();

  expect(storageMap.core.knownState()).toEqual(updateErrored.core.knownState());
});
