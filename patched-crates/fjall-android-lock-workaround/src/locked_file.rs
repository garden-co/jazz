// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::{fs::File, path::Path, sync::Arc};

enum LockMode {
    Locked(File),
    #[cfg(target_os = "android")]
    Unsupported,
}

struct LockedFileGuardInner(LockMode);

enum LockStatus {
    Acquired,
    #[cfg(target_os = "android")]
    Unsupported,
}

impl Drop for LockedFileGuardInner {
    fn drop(&mut self) {
        let file = match &self.0 {
            LockMode::Locked(file) => file,
            #[cfg(target_os = "android")]
            LockMode::Unsupported => return,
        };

        log::debug!("Unlocking database lock");

        file.unlock()
            .inspect_err(|e| {
                log::warn!("Failed to unlock database lock: {e:?}");
            })
            .ok();
    }
}

#[derive(Clone)]
#[expect(unused)]
pub struct LockedFileGuard(Arc<LockedFileGuardInner>);

impl LockedFileGuard {
    fn guard_from_status(file: File, status: LockStatus) -> Self {
        match status {
            LockStatus::Acquired => Self(Arc::new(LockedFileGuardInner(LockMode::Locked(file)))),
            #[cfg(target_os = "android")]
            LockStatus::Unsupported => Self(Arc::new(LockedFileGuardInner(LockMode::Unsupported))),
        }
    }

    fn try_lock(file: &File) -> crate::Result<LockStatus> {
        match file.try_lock() {
            Ok(()) => Ok(LockStatus::Acquired),
            Err(std::fs::TryLockError::WouldBlock) => Err(crate::Error::Locked),
            Err(std::fs::TryLockError::Error(error)) => Self::handle_lock_error(error),
        }
    }

    #[cfg(target_os = "android")]
    fn handle_lock_error(error: std::io::Error) -> crate::Result<LockStatus> {
        if error.kind() == std::io::ErrorKind::Unsupported {
            // Android's std file locking currently reports Unsupported.
            log::warn!(
                "Database file locking is unsupported on Android; proceeding without an inter-process lock"
            );
            return Ok(LockStatus::Unsupported);
        }

        log::error!("Failed to acquire database lock - if this is expected, you can try opening again (maybe wait a little)");
        Err(crate::Error::Io(error))
    }

    #[cfg(not(target_os = "android"))]
    fn handle_lock_error(error: std::io::Error) -> crate::Result<LockStatus> {
        log::error!("Failed to acquire database lock - if this is expected, you can try opening again (maybe wait a little)");
        Err(crate::Error::Io(error))
    }

    pub fn create_new(path: &Path) -> crate::Result<Self> {
        log::debug!("Acquiring database lock at {}", path.display());

        let file = match File::create_new(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => File::open(path)?,
            e => e?,
        };

        let status = Self::try_lock(&file)?;

        Ok(Self::guard_from_status(file, status))
    }

    pub fn try_acquire(path: &Path) -> crate::Result<Self> {
        const RETRIES: usize = 3;

        log::debug!("Acquiring database lock at {}", path.display());

        let file = File::open(path)?;

        for i in 1..=RETRIES {
            match Self::try_lock(&file) {
                Ok(status) => return Ok(Self::guard_from_status(file, status)),
                Err(crate::Error::Locked) => {
                    if i == RETRIES {
                        return Err(crate::Error::Locked);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                Err(error) => return Err(error),
            }
        }

        unreachable!("lock retry loop should return on success or error")
    }
}
