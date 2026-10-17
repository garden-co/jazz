# Persistent encoding and root-profile registry

The registry keeps individual encoding contracts and their byte-level,
rejection, and reopen evidence. A `profile` field marks an actual manifest
member; untagged encoding receipts remain mandatory audit coverage without
adding per-record compatibility markers.

Jazz has three independent profiles: `jazz-node-root`,
`server-catalogue-root`, and `account-root`. Each contains one Jazz storage
format marker. Node roots also declare Groove's durable-index format and
Groove's dictionary format (`groove.dictionary.v1`, interned author columns);
all roots compose Groove's mandatory base profile. See
`storage_epoch_1_jazz_codec_profile.md` for exact manifest receipts.
