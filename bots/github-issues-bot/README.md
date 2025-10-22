# Discord github issues bot

## Create github issues on the go

![Apr-27-2022 04-56-27](https://user-images.githubusercontent.com/32592458/165409043-8d7fff7a-79b7-403b-b2c8-cf4cdc8ce65b.gif)

## Prerequisites

1. Create an account on discord developer portal and add an application
2. Create a bot
3. Invite the bot to your server

## Installation steps

1. Create a new file `.env` in the project root directory and copy the contents from `.env.example`.
2. Update the `.env` file with relevant configurations.
    - BOT_TOKEN: discord bot token that is present in the discord developers section https://www.writebots.com/discord-bot-token/
    - GITHUB_ACCESS_TOKEN: Token used to authenticate with github https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token
    - GUILD_ID: Right click on the server name. At the last their should be an option for copy ID
        <img width="359" alt="Screenshot 2022-05-26 at 9 13 36 AM" src="https://user-images.githubusercontent.com/32592458/170412419-9355fefe-edea-45f2-91b7-b8a5934f9028.png">

    - GITHUB_USERNAME: The repository organization or username. Ex. In this https://github.com/mdshamoon/glific-frontend. the username is `mdshamoon`
    - GITHUB_REPOSITORY: the name of the repository. Ex. In this https://github.com/mdshamoon/glific-frontend repo name is `glific-frontend`
3. Run `pnpm install`

## Hosting steps

Host it on a server of your choice by running `pnpm start`.