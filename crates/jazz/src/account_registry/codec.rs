//! Closed v1 account registry command encoding. No serde-dependent storage tags.
use super::{AccountCommand, AccountId, Principal};
use uuid::Uuid;

const MAGIC: &[u8] = b"JACC\x01";
const MAX_COMPONENT: usize = 16 * 1024;

pub(super) fn encode(command: &AccountCommand) -> Result<Vec<u8>, String> {
    let mut out = MAGIC.to_vec();
    match command {
        AccountCommand::FoundLocalFirst { principal, app } => {
            out.push(4);
            put_principal(&mut out, principal)?;
            out.extend_from_slice(app.as_bytes());
        }
        AccountCommand::Register { principal, account } => {
            out.push(0);
            put_principal(&mut out, principal)?;
            out.extend_from_slice(account.0.as_bytes());
        }
        AccountCommand::RequestLink {
            approver,
            candidate,
            nonce,
            now,
            expires_at,
        } => {
            out.push(1);
            put_principal(&mut out, approver)?;
            put_principal(&mut out, candidate)?;
            out.extend_from_slice(nonce.as_bytes());
            out.extend_from_slice(&now.to_be_bytes());
            out.extend_from_slice(&expires_at.to_be_bytes());
        }
        AccountCommand::AcceptLink {
            candidate,
            nonce,
            now,
        } => {
            out.push(2);
            put_principal(&mut out, candidate)?;
            out.extend_from_slice(nonce.as_bytes());
            out.extend_from_slice(&now.to_be_bytes());
        }
        AccountCommand::Revoke { approver, target } => {
            out.push(3);
            put_principal(&mut out, approver)?;
            put_principal(&mut out, target)?;
        }
    }
    Ok(out)
}

fn put_principal(out: &mut Vec<u8>, value: &Principal) -> Result<(), String> {
    for component in [&value.issuer, &value.subject] {
        if component.len() > MAX_COMPONENT
            || !crate::tools::identity::principal_is_nonempty(component)
        {
            return Err("invalid account principal component".into());
        }
        out.extend_from_slice(&(component.len() as u32).to_be_bytes());
        out.extend_from_slice(component.as_bytes());
    }
    Ok(())
}

pub(super) fn decode(bytes: &[u8]) -> Result<AccountCommand, String> {
    let mut reader = Reader(
        bytes
            .strip_prefix(MAGIC)
            .ok_or("unsupported account command encoding")?,
    );
    let command = match reader.take(1)?[0] {
        0 => AccountCommand::Register {
            principal: reader.principal()?,
            account: AccountId(reader.uuid()?),
        },
        1 => AccountCommand::RequestLink {
            approver: reader.principal()?,
            candidate: reader.principal()?,
            nonce: reader.uuid()?,
            now: reader.u64()?,
            expires_at: reader.u64()?,
        },
        2 => AccountCommand::AcceptLink {
            candidate: reader.principal()?,
            nonce: reader.uuid()?,
            now: reader.u64()?,
        },
        3 => AccountCommand::Revoke {
            approver: reader.principal()?,
            target: reader.principal()?,
        },
        4 => AccountCommand::FoundLocalFirst {
            principal: reader.principal()?,
            app: reader.uuid()?,
        },
        _ => return Err("unknown account command tag".into()),
    };
    if !reader.0.is_empty() {
        return Err("trailing account command bytes".into());
    }
    if encode(&command)? != bytes {
        return Err("noncanonical account command".into());
    }
    Ok(command)
}

struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], String> {
        if self.0.len() < count {
            return Err("truncated account command".into());
        }
        let (head, rest) = self.0.split_at(count);
        self.0 = rest;
        Ok(head)
    }
    fn uuid(&mut self) -> Result<Uuid, String> {
        Ok(Uuid::from_bytes(
            self.take(16)?.try_into().expect("length checked"),
        ))
    }
    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("length checked"),
        ))
    }
    fn string(&mut self) -> Result<String, String> {
        let length = u32::from_be_bytes(self.take(4)?.try_into().expect("length checked")) as usize;
        if length > MAX_COMPONENT {
            return Err("account principal exceeds limit".into());
        }
        String::from_utf8(self.take(length)?.to_vec())
            .map_err(|_| "invalid account principal UTF-8".into())
    }
    fn principal(&mut self) -> Result<Principal, String> {
        Ok(Principal {
            issuer: self.string()?,
            subject: self.string()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Pin actual durable bytes, not a serde round trip of an implementation type.
    #[test]
    fn v1_registration_bytes_and_rejections_are_pinned() {
        let command = AccountCommand::Register {
            principal: Principal {
                issuer: "i".into(),
                subject: "s".into(),
            },
            account: AccountId(Uuid::from_u128(1)),
        };
        let golden = b"JACC\x01\x00\x00\x00\x00\x01i\x00\x00\x00\x01s\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01";
        assert_eq!(encode(&command).unwrap(), golden);
        assert_eq!(encode(&decode(golden).unwrap()).unwrap(), golden);
        for length in 0..golden.len() {
            assert!(decode(&golden[..length]).is_err());
        }
        let mut trailing = golden.to_vec();
        trailing.push(0);
        assert!(decode(&trailing).is_err());
        let mut unknown = golden.to_vec();
        unknown[5] = 5;
        assert!(decode(&unknown).is_err());
        let mut version = golden.to_vec();
        version[4] = 2;
        assert!(decode(&version).is_err());
    }
}
