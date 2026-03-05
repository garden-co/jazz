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
    landerSpawnX INTEGER NOT NULL,
    thrusting BOOLEAN NOT NULL
);
CREATE POLICY players_select_policy ON players FOR SELECT USING (TRUE);
CREATE POLICY players_insert_policy ON players FOR INSERT WITH CHECK (TRUE);
CREATE POLICY players_update_policy ON players FOR UPDATE USING (TRUE) WITH CHECK (TRUE);
CREATE POLICY players_delete_policy ON players FOR DELETE USING (TRUE);

CREATE TABLE fuel_deposits (
    fuelType TEXT NOT NULL,
    positionX INTEGER NOT NULL,
    createdAt INTEGER NOT NULL,
    collected BOOLEAN NOT NULL,
    collectedBy TEXT NOT NULL
);
CREATE POLICY fuel_deposits_select_policy ON fuel_deposits FOR SELECT USING (TRUE);
CREATE POLICY fuel_deposits_insert_policy ON fuel_deposits FOR INSERT WITH CHECK (TRUE);
CREATE POLICY fuel_deposits_update_policy ON fuel_deposits FOR UPDATE USING (TRUE) WITH CHECK (TRUE);
CREATE POLICY fuel_deposits_delete_policy ON fuel_deposits FOR DELETE USING (TRUE);

CREATE TABLE chat_messages (
    playerId TEXT NOT NULL,
    message TEXT NOT NULL,
    createdAt INTEGER NOT NULL
);
CREATE POLICY chat_messages_select_policy ON chat_messages FOR SELECT USING (TRUE);
CREATE POLICY chat_messages_insert_policy ON chat_messages FOR INSERT WITH CHECK (TRUE);
