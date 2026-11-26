//! The post-quantum mechanism a commitment record names.

use crate::envelope::Mechanism;

pub const MECHANISM: Mechanism<'static> = Mechanism::new("jazz.commit.slh-dsa-sha2-128s", 1);
