# TransportManager reconnect loop leaks on early disconnect

## What

`connect()` spawns `manager.run()` and drops the `JoinHandle` immediately, but `TransportManager::run` only exits on dropped handle inside `run_connected` and explicitly requires aborting during connect/backoff phases. If `disconnect()` happens before a successful connection (or while reconnecting), the task keeps retrying indefinitely, leaking background reconnect loops across shutdown/reconnect cycles.

## Priority

high

## Notes
