//! Blocking adapters for tests written against Jazz's former synchronous API.
//! New async lifecycle tests poll futures directly instead, so suspension and
//! ordering remain observable.
#![allow(missing_docs)]

use std::future::Future;

use crate::local_executor::block_on;

#[allow(clippy::wrong_self_convention)]
pub trait ResultFutureExt<T, E>: Future<Output = Result<T, E>> {
    fn unwrap(self) -> T
    where
        Self: Sized,
        E: std::fmt::Debug,
    {
        block_on(self).unwrap()
    }

    fn expect(self, message: &str) -> T
    where
        Self: Sized,
        E: std::fmt::Debug,
    {
        block_on(self).expect(message)
    }

    fn unwrap_or_else<F>(self, op: F) -> T
    where
        Self: Sized,
        F: FnOnce(E) -> T,
    {
        block_on(self).unwrap_or_else(op)
    }

    fn unwrap_err(self) -> E
    where
        Self: Sized,
        T: std::fmt::Debug,
    {
        block_on(self).unwrap_err()
    }

    fn expect_err(self, message: &str) -> E
    where
        Self: Sized,
        T: std::fmt::Debug,
    {
        block_on(self).expect_err(message)
    }

    fn is_err(self) -> bool
    where
        Self: Sized,
    {
        block_on(self).is_err()
    }

    fn is_ok(self) -> bool
    where
        Self: Sized,
    {
        block_on(self).is_ok()
    }
}

impl<F, T, E> ResultFutureExt<T, E> for F where F: Future<Output = Result<T, E>> {}

#[allow(clippy::wrong_self_convention)]
pub trait OptionFutureExt<T>: Future<Output = Option<T>> {
    fn unwrap(self) -> T
    where
        Self: Sized,
    {
        block_on(self).unwrap()
    }

    fn expect(self, message: &str) -> T
    where
        Self: Sized,
    {
        block_on(self).expect(message)
    }

    fn is_none(self) -> bool
    where
        Self: Sized,
    {
        block_on(self).is_none()
    }
}

impl<F, T> OptionFutureExt<T> for F where F: Future<Output = Option<T>> {}

pub trait FutureResolveExt: Future {
    fn resolve(self) -> Self::Output
    where
        Self: Sized,
    {
        block_on(self)
    }
}

impl<F> FutureResolveExt for F where F: Future {}
