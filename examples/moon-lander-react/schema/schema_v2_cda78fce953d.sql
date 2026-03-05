CREATE TABLE chat_messages (
    playerId TEXT NOT NULL,
    message TEXT NOT NULL,
    createdAt INTEGER NOT NULL
);

CREATE TABLE fuel_deposits (
    fuelType TEXT NOT NULL,
    positionX INTEGER NOT NULL,
    createdAt INTEGER NOT NULL,
    collected BOOLEAN NOT NULL,
    collectedBy TEXT NOT NULL
);

CREATE TABLE players (
    playerId TEXT NOT NULL,
    name TEXT NOT NULL,
    color TEXT NOT NULL,
    mode TEXT NOT NULL,
    online BOOLEAN NOT NULL,
    lastSeen INTEGER NOT NULL,
    positionX INTEGER NOT NULL,
    positionY INTEGER NOT NULL,
    velocityX INTEGER NOT NULL,
    velocityY INTEGER NOT NULL,
    requiredFuelType TEXT NOT NULL,
    landerFuelLevel INTEGER NOT NULL,
    landerSpawnX INTEGER NOT NULL
);