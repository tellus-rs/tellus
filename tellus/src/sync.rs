#[cfg(feature = "cluster")]
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{
    ops::Deref,
    sync::{Mutex, MutexGuard, PoisonError},
};

#[repr(align(128))]
pub(crate) struct CachePadded<T>(T);

impl<T> CachePadded<T> {
    pub(crate) fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T> Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(feature = "cluster")]
pub(crate) fn read<T>(rw_lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    rw_lock.read().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(feature = "cluster")]
pub(crate) fn write<T>(rw_lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    rw_lock.write().unwrap_or_else(PoisonError::into_inner)
}
