//! Registry v1 uses the Groove record/enum algebra, with no private byte codec.
use super::{AccountCommand, AccountId, Principal};
use crate::groove::records::{
    EnumCase, EnumSchema, EnumValue, OwnedRecord, RecordDescriptor, Value, ValueType,
};

const MAX_COMPONENT: usize = 16 * 1024;

fn principal_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([
        ("issuer", ValueType::String),
        ("subject", ValueType::String),
    ])
}

fn schema() -> EnumSchema {
    let principal = || ValueType::Record(Box::new(principal_descriptor()));
    EnumSchema::new(
        "jazz.account-command.v1",
        [
            EnumCase::new(
                "Register",
                RecordDescriptor::new([("principal", principal()), ("account", ValueType::Uuid)]),
            ),
            EnumCase::new(
                "RequestLink",
                RecordDescriptor::new([
                    ("approver", principal()),
                    ("candidate", principal()),
                    ("nonce", ValueType::Uuid),
                    ("now", ValueType::U64),
                    ("expires_at", ValueType::U64),
                ]),
            ),
            EnumCase::new(
                "AcceptLink",
                RecordDescriptor::new([
                    ("candidate", principal()),
                    ("nonce", ValueType::Uuid),
                    ("now", ValueType::U64),
                ]),
            ),
            EnumCase::new(
                "Revoke",
                RecordDescriptor::new([("approver", principal()), ("target", principal())]),
            ),
            EnumCase::new(
                "FoundLocalFirst",
                RecordDescriptor::new([("principal", principal()), ("app", ValueType::Uuid)]),
            ),
        ],
    )
    .expect("closed account command schema")
    .with_registry_id(1)
}

pub(super) fn descriptor() -> RecordDescriptor {
    RecordDescriptor::new([("command", ValueType::Enum(Box::new(schema())))])
}

fn principal_value(principal: &Principal) -> Result<Value, String> {
    for component in [&principal.issuer, &principal.subject] {
        if component.len() > MAX_COMPONENT
            || !crate::tools::identity::principal_is_nonempty(component)
        {
            return Err("invalid account principal component".into());
        }
    }
    let descriptor = principal_descriptor();
    let bytes = descriptor
        .create(&[
            Value::String(principal.issuer.clone()),
            Value::String(principal.subject.clone()),
        ])
        .map_err(error)?;
    Ok(Value::Record(OwnedRecord::new(bytes, descriptor)))
}

pub(super) fn encode(command: &AccountCommand) -> Result<Vec<u8>, String> {
    let (tag, values) = match command {
        AccountCommand::Register { principal, account } => {
            (0, vec![principal_value(principal)?, Value::Uuid(account.0)])
        }
        AccountCommand::RequestLink {
            approver,
            candidate,
            nonce,
            now,
            expires_at,
        } => (
            1,
            vec![
                principal_value(approver)?,
                principal_value(candidate)?,
                Value::Uuid(*nonce),
                Value::U64(*now),
                Value::U64(*expires_at),
            ],
        ),
        AccountCommand::AcceptLink {
            candidate,
            nonce,
            now,
        } => (
            2,
            vec![
                principal_value(candidate)?,
                Value::Uuid(*nonce),
                Value::U64(*now),
            ],
        ),
        AccountCommand::Revoke { approver, target } => (
            3,
            vec![principal_value(approver)?, principal_value(target)?],
        ),
        AccountCommand::FoundLocalFirst { principal, app } => {
            (4, vec![principal_value(principal)?, Value::Uuid(*app)])
        }
    };
    let payload = schema().cases[tag as usize].payload;
    descriptor()
        .create(&[Value::Enum(
            EnumValue::create(tag, payload, &values).map_err(error)?,
        )])
        .map_err(error)
}

fn principal(value: &Value) -> Result<Principal, String> {
    let Value::Record(record) = value else {
        return Err("invalid account principal record".into());
    };
    let values = record.to_values().map_err(error)?;
    let [Value::String(issuer), Value::String(subject)] = values.as_slice() else {
        return Err("invalid account principal fields".into());
    };
    Ok(Principal {
        issuer: issuer.clone(),
        subject: subject.clone(),
    })
}

pub(super) fn decode(bytes: &[u8]) -> Result<AccountCommand, String> {
    let Value::Enum(command) = descriptor().get_idx(bytes, 0).map_err(error)? else {
        return Err("invalid account command record".into());
    };
    let values = command.record().to_values().map_err(error)?;
    let command = match (command.tag(), values.as_slice()) {
        (0, [p, Value::Uuid(account)]) => AccountCommand::Register {
            principal: principal(p)?,
            account: AccountId(*account),
        },
        (
            1,
            [
                a,
                c,
                Value::Uuid(nonce),
                Value::U64(now),
                Value::U64(expires_at),
            ],
        ) => AccountCommand::RequestLink {
            approver: principal(a)?,
            candidate: principal(c)?,
            nonce: *nonce,
            now: *now,
            expires_at: *expires_at,
        },
        (2, [c, Value::Uuid(nonce), Value::U64(now)]) => AccountCommand::AcceptLink {
            candidate: principal(c)?,
            nonce: *nonce,
            now: *now,
        },
        (3, [a, t]) => AccountCommand::Revoke {
            approver: principal(a)?,
            target: principal(t)?,
        },
        (4, [p, Value::Uuid(app)]) => AccountCommand::FoundLocalFirst {
            principal: principal(p)?,
            app: *app,
        },
        _ => return Err("unknown account command".into()),
    };
    // Validate principals and reject noncanonical Groove record representations.
    if encode(&command)? != bytes {
        return Err("noncanonical account command".into());
    }
    Ok(command)
}

fn error(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    // The durable descriptor and bytes are not observable through admission APIs:
    // pin them internally so coupled encoder/decoder changes cannot hide drift.
    #[test]
    fn v1_command_and_descriptor_corpus() {
        let a = Principal {
            issuer: "i".into(),
            subject: "s".into(),
        };
        let b = Principal {
            issuer: "j".into(),
            subject: "λ".into(),
        };
        let nonce = Uuid::from_bytes([2; 16]);
        let commands = [
            AccountCommand::Register {
                principal: a.clone(),
                account: AccountId(Uuid::from_u128(1)),
            },
            AccountCommand::RequestLink {
                approver: a.clone(),
                candidate: b.clone(),
                nonce,
                now: 0x0102030405060708,
                expires_at: 0x1112131415161718,
            },
            AccountCommand::AcceptLink {
                candidate: b.clone(),
                nonce,
                now: 0x0102030405060708,
            },
            AccountCommand::Revoke {
                approver: a.clone(),
                target: b,
            },
            AccountCommand::FoundLocalFirst {
                principal: a,
                app: nonce,
            },
        ];
        let mut corpus = String::new();
        for command in commands {
            let bytes = encode(&command).unwrap();
            corpus.push_str(&hex(&bytes));
            corpus.push('\n');
            assert_eq!(decode(&bytes).unwrap(), command);
            for end in 0..bytes.len() {
                assert!(decode(&bytes[..end]).is_err());
            }
            let mut trailing = bytes;
            trailing.push(0xff);
            assert!(decode(&trailing).is_err());
        }
        corpus.push_str(&hex(&crate::groove::records::encode_record_descriptor(
            &descriptor(),
        )
        .unwrap()));
        corpus.push('\n');
        assert_eq!(corpus, include_str!("command-v1.corpus"));
        assert!(decode(b"JACC\x01\x00").is_err());
        // A terminal Groove String consumes its record remainder. Valid UTF-8
        // suffixes change that field; they are not malformed framing.
        let command = AccountCommand::Register {
            principal: Principal {
                issuer: "i".into(),
                subject: "s".into(),
            },
            account: AccountId(Uuid::from_u128(1)),
        };
        let mut extended = encode(&command).unwrap();
        extended.push(b'x');
        assert_eq!(
            decode(&extended).unwrap(),
            AccountCommand::Register {
                principal: Principal {
                    issuer: "i".into(),
                    subject: "sx".into()
                },
                account: AccountId(Uuid::from_u128(1)),
            }
        );
    }

    // Invalid on-disk principals require bypassing the valid command constructor.
    #[test]
    fn v2_rejects_unknown_tags_and_invalid_principals() {
        assert!(decode(&[5]).is_err());
        assert!(decode(&[0x80, 0]).is_err());
        for issuer in ["".to_owned(), " ".to_owned(), "i".repeat(MAX_COMPONENT + 1)] {
            let principal_descriptor = principal_descriptor();
            let raw = principal_descriptor
                .create(&[Value::String(issuer.clone()), Value::String("s".into())])
                .unwrap();
            let values = [
                Value::Record(OwnedRecord::new(raw, principal_descriptor)),
                Value::Uuid(Uuid::from_u128(1)),
            ];
            let bytes = descriptor()
                .create(&[Value::Enum(
                    EnumValue::create(0, schema().cases[0].payload, &values).unwrap(),
                )])
                .unwrap();
            assert!(decode(&bytes).is_err());
            assert!(
                encode(&AccountCommand::Register {
                    principal: Principal {
                        issuer,
                        subject: "s".into()
                    },
                    account: AccountId(Uuid::from_u128(1))
                })
                .is_err()
            );
        }
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}
