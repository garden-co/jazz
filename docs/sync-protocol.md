# Jazz Sync Protocol

## Overview

The Jazz sync protocol uses four message types:
- **LOAD**: Request to load/subscribe to a CoValue
- **KNOWN**: Share known state (what transactions we have)
- **CONTENT**: Send new transactions/content
- **DONE**: Signal completion (not used at the moment)

## Message Type Details

### LOAD Message
- **Purpose**: Subscribe to a CoValue and request sync
- **Contains**: Known state (what the sender already has)
- **Response**:
  - CONTENT missing from the sender
  - KNOWN state if the sender is not missing any content

### KNOWN Message
- **Purpose**: Share known state (acknowledgment or state update)
- **Contains**: Current known state (header + session transaction counts)
- **Variants**: 
  - Normal: `{action: "known", ...}`
  - Correction: `{action: "known", isCorrection: true, ...}`
- **Response**: CONTENT missing from the sender's known state

### CONTENT Message
- **Purpose**: Send new transactions
- **Contains**: 
  - Header (if first time sending this CoValue)
  - New transactions per session: `{after: N, newTransactions: [...]}`
  - `expectContentUntil` (for streaming large CoValues)
- **Response**:
  - KNOWN message as acknowledgment
  - KNOWN with `isCorrection: true` if missing CoValue content
  - LOAD request for any missing CoValue dependency
  - CONTENT with empty known state if missing the CoValue

### DONE Message
- **Purpose**: Signal completion (not used at the moment)
- **Contains**: Just the CoValue ID

## Common Sync Flows

### Initial Connection - Client Subscribes to CoValue

```mermaid
sequenceDiagram
    participant Client
    participant Server
        
    Client->>Server: LOAD {id, header: false, sessions: {}}
    Note right of Client: Client has no data yet
    
    alt Server has CoValue
        Server->>Server: CoValue is available in memory, storage or peer
        Server->>Client: CONTENT {id, header: true, new: {...}}
        Note left of Server: Sends header + all transactions
        Client->>Client: Apply transactions
        Client->>Server: KNOWN {id, header: true, sessions: {...}}
        Note right of Client: Acknowledge receipt with current state
    else Server doesn't have CoValue
        Server->>Client: KNOWN {id, header: false, sessions: {}}
        Note left of Server: Signals CoValue not found
    end
```

### Client with CoValue subscribes to Server
```mermaid
sequenceDiagram
    participant Client
    participant Server
    
    Note over Client,Server: Initial Connection & Subscription
    
    Client->>Server: LOAD {id, knownState}
    alt Server has new content
        Server->>Client: CONTENT {id, header, newTransactions}
        Client->>Server: KNOWN {id, updatedState}
    else Client already up-to-date
        Server->>Client: KNOWN {id, knownState}
    else Server missing content
        Server->>Client: KNOWN {id, knownState}
        Client->>Server: CONTENT {id, header, newTransactions}
    end
    
    Note over Client,Server: Client Creates New Content
    
    Client->>Client: Create transaction
    Client->>Server: CONTENT {id, newTransactions}
    Note right of Client: Sends only new transactions
    alt Valid state
        Server->>Server: Apply & store
        Server->>Client: KNOWN {id, updatedState}
    else Invalid state (correction)
        Server->>Client: KNOWN {id, isCorrection: true}
        Client->>Server: CONTENT {id, fullContent}
        Server->>Client: KNOWN {id, updatedState}
    end
    
    Note over Client,Server: Server Pushes Updates
    
    Server->>Server: Receive CONTENT from Client2
    Server->>Client: CONTENT {id, newTransactions}
    Client->>Server: KNOWN {id, updatedState}
```
