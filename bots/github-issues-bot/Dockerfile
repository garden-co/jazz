# bots/github-issues-bot/Dockerfile
FROM node:22-slim

# Set working directory
WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev

# Copy the source
COPY . .

# Build (if using TypeScript)
RUN npm run build

# Set environment variables
ENV NODE_ENV=production

# Start the bot
CMD ["node", "dist/index.js"]
