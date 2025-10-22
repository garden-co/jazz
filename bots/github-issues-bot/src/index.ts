import { Client, GatewayIntentBits, ApplicationCommandType } from "discord.js";
import dotenv from "dotenv";
import { getModal } from "./utils";
import express from "express";
import fetch from "node-fetch";

type IssueResponse = {
  url: string;
};

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

app.get("/", (req, res) => res.send("Github issues bot!"));
app.listen(PORT, () => console.log(`Server listening on ${PORT}`));

const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages],
});

client.on("clientReady", async () => {
  console.log("Issue bot ready");
  const guildId = process.env.GUILD_ID || "";
  const guild = client.guilds.cache.get(guildId);
  const commands = guild ? guild.commands : client.application?.commands;

  await commands?.create({
    name: "Open github issue",
    type: ApplicationCommandType.Message,
  });
});

client.on("interactionCreate", async (interaction) => {
  if (
    !interaction.isMessageContextMenuCommand() &&
    !interaction.isModalSubmit()
  )
    return;

  const channel = interaction.channel;
  if (!channel || !channel.isTextBased()) return;

  if (interaction.isMessageContextMenuCommand()) {
    const modal = getModal(interaction.targetMessage.content);
    interaction.showModal(modal);
  } else if (interaction.isModalSubmit()) {
    const fields = interaction.fields;
    const issueTitle = fields.getTextInputValue("issueTitle");
    const issueDescription = fields.getTextInputValue("issueDescription");

    if (!issueTitle?.trim() || !issueDescription?.trim()) {
      await interaction.reply({
        content: "Please fill out both the title and description fields.",
        ephemeral: true,
      });
      return;
    }

    try {
      const response = await fetch(
        "https://github-issues-bot.vercel.app/api/create-issue",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ title: issueTitle, body: issueDescription }),
        },
      );

      if (!response.ok) {
        const text = await response.text();
        console.error("GitHub API error:", text);
        return await interaction.reply(
          "Failed to create issue. See console for details.",
        );
      }

      const data = (await response.json()) as IssueResponse;
      if (data.url) {
        await interaction.reply(`Issue created: ${data.url}`);
      }
    } catch (err) {
      console.error("Fetch error:", err);
      await interaction.reply("An error occurred while creating the issue.");
    }
  }
});

client.login(process.env.BOT_TOKEN);
