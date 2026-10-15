//! Bounded byte budget shared by every in-flight aligned read chunk.
//!
//! The uploader never buffers a whole segment: the reader thread acquires a byte
//! permit before it allocates an aligned chunk and the permit is released when the
//! last `Bytes` handle for that chunk is dropped by the blob store. Because the
//! channel holds owned chunks, the budget bounds total reader memory no matter how
//! far ahead of the S3 PUT the reader runs.

use std::{
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug, Default)]
struct Inner {
    in_use: usize,
    high_water: usize,
}

/// A counting semaphore over bytes. Cloning is not supported; share with `Arc`.
#[derive(Debug)]
pub struct ByteBudget {
    capacity: usize,
    inner: Mutex<Inner>,
    available: Condvar,
}

impl ByteBudget {
    /// Create a budget with `capacity` bytes; returns a shared handle.
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            inner: Mutex::new(Inner::default()),
            available: Condvar::new(),
        })
    }

    /// Total bytes the budget may hand out.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Bytes currently leased.
    pub fn in_use(&self) -> usize {
        self.lock().in_use
    }

    /// Largest number of bytes ever leased at once, for accounting tests.
    pub fn high_water(&self) -> usize {
        self.lock().high_water
    }

    /// Lease `bytes` without waiting, returning `None` when the budget is
    /// temporarily exhausted or `bytes` exceeds the whole capacity.
    pub fn try_acquire(self: &Arc<Self>, bytes: usize) -> Option<BudgetGuard> {
        if bytes > self.capacity {
            return None;
        }
        let mut inner = self.lock();
        if inner.in_use + bytes <= self.capacity {
            inner.in_use += bytes;
            inner.high_water = inner.high_water.max(inner.in_use);
            Some(BudgetGuard {
                budget: Arc::clone(self),
                bytes,
            })
        } else {
            None
        }
    }

    /// Lease `bytes`, blocking the calling (non-async) thread until the budget
    /// has room. Returns `None` when `bytes` exceeds the whole capacity, which is
    /// a configuration error rather than transient backpressure.
    pub fn acquire(self: &Arc<Self>, bytes: usize) -> Option<BudgetGuard> {
        if bytes > self.capacity {
            return None;
        }
        let mut inner = self.lock();
        while inner.in_use + bytes > self.capacity {
            inner = self
                .available
                .wait_timeout(inner, Duration::from_millis(250))
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
        inner.in_use += bytes;
        inner.high_water = inner.high_water.max(inner.in_use);
        Some(BudgetGuard {
            budget: Arc::clone(self),
            bytes,
        })
    }

    /// Wait until at least `bytes` are free, without leasing them. Useful for
    /// tests and for graceful shutdown paths.
    pub fn wait_until_free(&self, bytes: usize, timeout: Duration) -> bool {
        if bytes > self.capacity {
            return false;
        }
        let deadline = Instant::now() + timeout;
        let mut inner = self.lock();
        while inner.in_use + bytes > self.capacity {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            inner = self
                .available
                .wait_timeout(inner, deadline - now)
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
        true
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn release(&self, bytes: usize) {
        {
            let mut inner = self.lock();
            inner.in_use = inner.in_use.saturating_sub(bytes);
        }
        self.available.notify_all();
    }
}

/// RAII lease of `bytes` from a [`ByteBudget`]. Dropping the last owner of a
/// read chunk releases the permit.
#[derive(Debug)]
pub struct BudgetGuard {
    budget: Arc<ByteBudget>,
    bytes: usize,
}

impl BudgetGuard {
    /// Bytes leased by this guard.
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for BudgetGuard {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicBool, Ordering},
        thread,
    };

    use super::*;

    #[test]
    fn try_acquire_respects_capacity() {
        let budget = ByteBudget::new(8192);
        let first = budget.try_acquire(4096).expect("first chunk");
        let second = budget.try_acquire(4096).expect("second chunk");
        assert!(budget.try_acquire(1).is_none());
        assert_eq!(budget.in_use(), 8192);
        assert_eq!(budget.high_water(), 8192);

        drop(first);
        assert_eq!(budget.in_use(), 4096);
        assert!(budget.try_acquire(4096).is_some());
        drop(second);
    }

    #[test]
    fn oversize_request_is_rejected_without_blocking() {
        let budget = ByteBudget::new(4096);
        assert!(budget.try_acquire(8192).is_none());
        assert!(budget.acquire(8192).is_none());
        assert_eq!(budget.in_use(), 0);
    }

    #[test]
    fn blocking_acquire_waits_for_release() {
        let budget = ByteBudget::new(4096);
        let guard = budget.try_acquire(4096).unwrap();
        let released = Arc::new(AtomicBool::new(false));

        let waiter_budget = Arc::clone(&budget);
        let waiter_released = Arc::clone(&released);
        let waiter = thread::spawn(move || {
            let guard = waiter_budget.acquire(4096).expect("budget frees up");
            assert_eq!(guard.bytes(), 4096);
            waiter_released.store(true, Ordering::SeqCst);
        });

        thread::sleep(Duration::from_millis(50));
        assert!(!released.load(Ordering::SeqCst));
        drop(guard);
        waiter.join().unwrap();
        assert!(released.load(Ordering::SeqCst));
        assert_eq!(budget.in_use(), 0);
    }

    #[test]
    fn wait_until_free_times_out() {
        let budget = ByteBudget::new(1024);
        let _guard = budget.try_acquire(1024).unwrap();
        assert!(!budget.wait_until_free(1024, Duration::from_millis(20)));
    }
}
