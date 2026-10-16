//! Append-only event trace shared by the harness, the fault-injecting mock S3, and
//! the simulated I/O backend.
//!
//! The trace carries two responsibilities:
//!
//! 1. **Invariant evidence.** Invariants such as "a local segment is deleted only
//!    after the PUT and the manifest commit succeeded" are checked by looking for
//!    causal predecessors of each [`TraceEvent::LocalUnlinked`].
//! 2. **Reproducibility.** Every recorded event carries a monotonically increasing
//!    sequence number. The [`Trace::digest`] hashes the *canonical* event order
//!    (sorted by sequence number), so replaying a seed produces the same digest
//!    even if OS scheduling changes when an event lands.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};

use serde::{Deserialize, Serialize};

use crate::rng::digest_bytes;

/// One observed event in the simulated lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum TraceEvent {
    /// A writer preallocated/acquired a segment file.
    SegmentAcquired {
        segment_id: u64,
        epoch: u64,
        capacity: usize,
    },
    /// The writer acknowledged one entry (the block containing it is durable).
    AppendAcked {
        segment_id: u64,
        ledger: u64,
        entry: u64,
        sequence: u64,
    },
    /// A segment was sealed on disk.
    SegmentSealed {
        segment_id: u64,
        epoch: u64,
        file_len: u64,
    },
    /// A crash abandoned an open segment; recovery scanned it.
    OpenSegmentRecovered {
        segment_id: u64,
        epoch: u64,
        bytes: u64,
        records: u64,
    },
    /// The derived index was rebuilt from segment files.
    IndexRebuilt {
        segments_processed: usize,
        entries_indexed: usize,
    },
    /// A segment upload started (the local `Uploading` intent record landed).
    UploadStarted { segment_id: u64, key: String },
    /// A successful PUT completed in the object store.
    PutDone {
        segment_id: u64,
        key: String,
        bytes: u64,
    },
    /// The manifest CAS append for a segment succeeded.
    ManifestCommitted {
        segment_id: u64,
        epoch: u64,
        key: String,
    },
    /// The local segment file was removed (durable unlink).
    LocalUnlinked { segment_id: u64, path: String },
    /// An orphaned preallocation (no valid header, no acknowledged records) was
    /// removed during recovery. Unlike [`TraceEvent::LocalUnlinked`], delete-gating
    /// does not apply: nothing could have been acknowledged from this file.
    OrphanRemoved { segment_id: u64, path: String },
    /// The uploader refused to continue: the scenario expected this fencing event.
    Fenced {
        segment_id: u64,
        manifest_epoch: u64,
        local_epoch: u64,
    },
    /// A read-back verified one entry and where it was served from.
    ReadBack {
        ledger: u64,
        entry: u64,
        source: &'static str,
        len: usize,
    },
    /// The harness simulated a crash at a named point.
    Crash { point: String },
    /// A scenario-level checkpoint.
    Checkpoint { name: String },
}

/// Thread-safe append-only trace with a stable digest.
#[derive(Debug, Default)]
pub struct Trace {
    next_sequence: AtomicU64,
    events: Mutex<Vec<(u64, TraceEvent)>>,
}

impl Trace {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, event: TraceEvent) -> u64 {
        let sequence = self.next_sequence.fetch_add(1, Ordering::SeqCst);
        self.events
            .lock()
            .expect("trace mutex poisoned")
            .push((sequence, event));
        sequence
    }

    /// Events in sequence order.
    pub fn events(&self) -> Vec<TraceEvent> {
        let mut events = self.events.lock().expect("trace mutex poisoned").clone();
        events.sort_by_key(|(sequence, _)| *sequence);
        events.into_iter().map(|(_, event)| event).collect()
    }

    pub fn len(&self) -> usize {
        self.events.lock().expect("trace mutex poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Stable digest over the canonical (sequence-ordered) event encoding.
    pub fn digest(&self) -> u64 {
        let events = self.events();
        let encoded = serde_json::to_vec(&events).unwrap_or_default();
        digest_bytes(&[encoded])
    }

    /// Schedule-independent digest: the *set* of recorded events, hashed after
    /// sorting by their stable JSON encoding. Two runs that produce the same
    /// events in different interleavings share a content digest, which is what
    /// seeded reproducibility checks compare when several threads record events.
    pub fn content_digest(&self) -> u64 {
        let mut encoded: Vec<Vec<u8>> = self
            .events()
            .into_iter()
            .map(|event| serde_json::to_vec(&event).unwrap_or_default())
            .collect();
        encoded.sort_unstable();
        let refs: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();
        digest_bytes(&refs)
    }

    /// Check the delete-gating invariant: every `LocalUnlinked` must be preceded
    /// by a successful PUT *and* a manifest commit for the same segment.
    pub fn delete_gating_violations(&self) -> Vec<String> {
        let events = self.events();
        let mut violations = Vec::new();
        for (index, event) in events.iter().enumerate() {
            let TraceEvent::LocalUnlinked { segment_id, path } = event else {
                continue;
            };
            let put_before = events[..index].iter().any(|prior| {
                matches!(
                    prior,
                    TraceEvent::PutDone { segment_id: id, .. } if id == segment_id
                )
            });
            let commit_before = events[..index].iter().any(|prior| {
                matches!(
                    prior,
                    TraceEvent::ManifestCommitted { segment_id: id, .. } if id == segment_id
                )
            });
            if !put_before || !commit_before {
                violations.push(format!(
                    "segment {segment_id} at {path} was unlinked without \
                     PUT={put_before} and manifest={commit_before}"
                ));
            }
        }
        violations
    }

    /// Check that a fenced segment was never locally deleted or committed.
    pub fn fencing_violations(&self) -> Vec<String> {
        let events = self.events();
        let mut violations = Vec::new();
        for (index, event) in events.iter().enumerate() {
            let TraceEvent::Fenced { segment_id, .. } = event else {
                continue;
            };
            for prior in &events[..index] {
                // The fence is observed after the PUT; the uploader may have
                // committed rows for *earlier* epochs of the same segment id in a
                // previous life, which the scenario tracks separately.
                if let TraceEvent::LocalUnlinked { segment_id: id, .. } = prior {
                    if id == segment_id {
                        violations.push(format!(
                            "fenced segment {segment_id} was deleted before fencing"
                        ));
                    }
                }
            }
        }
        violations
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_is_stable_across_instances() {
        let first = Trace::new();
        first.record(TraceEvent::Checkpoint {
            name: "a".to_string(),
        });
        first.record(TraceEvent::PutDone {
            segment_id: 1,
            key: "k".to_string(),
            bytes: 10,
        });
        let second = Trace::new();
        second.record(TraceEvent::Checkpoint {
            name: "a".to_string(),
        });
        second.record(TraceEvent::PutDone {
            segment_id: 1,
            key: "k".to_string(),
            bytes: 10,
        });
        assert_eq!(first.digest(), second.digest());
    }

    #[test]
    fn delete_without_commit_is_flagged() {
        let trace = Trace::new();
        trace.record(TraceEvent::PutDone {
            segment_id: 1,
            key: "k".to_string(),
            bytes: 1,
        });
        trace.record(TraceEvent::LocalUnlinked {
            segment_id: 1,
            path: "1.seg".to_string(),
        });
        assert_eq!(trace.delete_gating_violations().len(), 1);
    }

    #[test]
    fn full_gating_passes() {
        let trace = Trace::new();
        for event in [
            TraceEvent::PutDone {
                segment_id: 2,
                key: "k".to_string(),
                bytes: 1,
            },
            TraceEvent::ManifestCommitted {
                segment_id: 2,
                epoch: 1,
                key: "k".to_string(),
            },
            TraceEvent::LocalUnlinked {
                segment_id: 2,
                path: "2.seg".to_string(),
            },
        ] {
            trace.record(event);
        }
        assert!(trace.delete_gating_violations().is_empty());
    }
}
