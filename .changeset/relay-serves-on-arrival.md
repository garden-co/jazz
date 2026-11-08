---
"jazz-tools": patch
---

Fix device relays that tick only when frames arrive (such as the React Native native relay) holding back what the server sent: a foreground's subscription now opens, and live changes arrive, in the relay tick that receives them instead of waiting for an unrelated later tick.
