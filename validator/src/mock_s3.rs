//! Deterministic, fault-injecting S3 stand-in for the validator harness.
//!
//! Unlike the uploader's `FaultyBlob` (a unit-test double with hand-set counters),
//! this store is built for seeded chaos runs:
//!
//! * fault decisions are a pure function of `(seed, operation kind, key, attempt)`,
//!   so replaying a seed reproduces every failure regardless of thread scheduling;
//! * it records a per-key ordered event log (and a digest of it) so a failing run
//!   has a stable fingerprint and the harness can check ordering invariants;
//! * it models S3's real hazards: transient errors *before* any byte is accepted,
//!   lost responses (the write landed but the caller saw an error), ETag/CAS races,
//!   and a competing writer that appends a newer-epoch manifest row between our PUT
//!   and our CAS;
//! * it can simulate a process death at a chosen successful PUT or manifest commit
//!   (the call reports a fatal error exactly when a real `kill -9` would have
//!   interrupted it) and an S3 outage window that fails every operation transiently.
//!
//! Latency spikes are *recorded*, and only slept when `real_latency` is set, so
//! deterministic runs never wait on modelled network time.

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Range,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bkxpro_uploader::{
    BlobError, BlobObject, BlobStore, ExpectedVersion, ManifestRecord, ObjectInfo, ObjectVersion,
    PutOutcome, StreamItem,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::rng::{digest_bytes, DetRng};

/// Tunable chaos rates, in percent (0-100).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MockS3Config {
    pub seed: u64,
    /// PUTs that fail transiently before any byte is accepted.
    pub put_fail_percent: u8,
    /// PUTs that store the object but report a transient error (lost response).
    pub put_lost_response_percent: u8,
    /// CAS writes that lose the version race without writing (injected conflict).
    pub cas_conflict_percent: u8,
    /// CAS writes that land but report a transient error (lost response).
    pub cas_lost_response_percent: u8,
    pub get_fail_percent: u8,
    pub head_fail_percent: u8,
    /// Base latency for every operation, in microseconds (recorded).
    pub latency_base_micros: u64,
    /// Uniform jitter added to the base latency.
    pub latency_jitter_micros: u64,
    /// Actually sleep the modelled latency (default false: record only).
    pub real_latency: bool,
}

impl Default for MockS3Config {
    fn default() -> Self {
        Self {
            seed: 0,
            put_fail_percent: 0,
            put_lost_response_percent: 0,
            cas_conflict_percent: 0,
            cas_lost_response_percent: 0,
            get_fail_percent: 0,
            head_fail_percent: 0,
            latency_base_micros: 0,
            latency_jitter_micros: 0,
            real_latency: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum S3Op {
    Put,
    Cas,
    Get,
    GetRange,
    Head,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum S3Outcome {
    Ok,
    Transient,
    Conflict,
    NotFound,
    Fatal,
}

/// What the seeded decision did to one operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Decision {
    /// Execute for real.
    Proceed,
    /// Fail before touching the store. For a PUT the caller's stream is dropped,
    /// so the reader observes a dead connection before any part was accepted.
    FailPreWrite(S3Outcome),
    /// The write lands, but the caller sees a transient error.
    LostResponse,
}

/// One recorded interaction with the store. `attempt` counts calls for the same
/// `(op, key)` pair, so per-key ordering is deterministic even when callers race.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MockEvent {
    pub sequence: u64,
    pub op: S3Op,
    pub key: String,
    pub attempt: u64,
    pub outcome: S3Outcome,
    pub bytes: u64,
    pub latency_micros: u64,
    /// Segment id parsed from the object key, when the key identifies one.
    pub segment_id: Option<u64>,
}

#[derive(Debug, Clone)]
struct StoredObject {
    bytes: Bytes,
    e_tag: String,
    version: String,
}

#[derive(Debug, Default)]
struct State {
    objects: BTreeMap<String, StoredObject>,
    attempts: BTreeMap<(S3Op, String), u64>,
    version_counter: u64,
    successful_puts: u64,
    successful_cas: u64,
    outage_remaining: u64,
    events: Vec<MockEvent>,
}

/// Chaos controls that a scenario arms before or between phases. They are
/// *counted on success*, so they are stable across caller retries and concurrency.
#[derive(Debug, Default)]
struct Armed {
    crash_after_successful_puts: BTreeSet<u64>,
    crash_after_successful_cas: BTreeSet<u64>,
    /// segment id -> competing writer epoch to append after our PUT lands.
    fence_after_put: BTreeMap<u64, u64>,
    /// (`after_ops`, `ops`): start a `ops`-call outage once this many operations
    /// have been recorded. Fires at most once.
    outage_after: Option<(u64, u64)>,
    outage_fired: bool,
}

#[derive(Debug)]
struct MockInner {
    config: MockS3Config,
    state: Mutex<State>,
    armed: Mutex<Armed>,
}

/// Cloneable handle. All clones share one store.
#[derive(Debug, Clone)]
pub struct MockS3 {
    inner: Arc<MockInner>,
}

/// Key of the manifest object; matches `bkxpro_uploader::DEFAULT_MANIFEST_KEY`.
pub const MANIFEST_KEY: &str = "manifest/log.jsonl";

/// Object key of a segment, mirroring the uploader's key scheme.
pub fn segment_key(segment_id: u64) -> String {
    format!("segments/{segment_id:020}.seg")
}

/// Parse an append-only JSONL manifest image; unparsable lines are skipped (the
/// manifest is append-only, so a torn final line is not a row yet).
pub fn parse_manifest(raw: &[u8]) -> Vec<ManifestRecord> {
    raw.split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .filter_map(|line| serde_json::from_slice::<ManifestRecord>(line).ok())
        .collect()
}

impl MockS3 {
    pub fn new(seed: u64) -> Self {
        let mut config = MockS3Config::default();
        config.seed = seed;
        Self::with_config(config)
    }

    pub fn with_config(config: MockS3Config) -> Self {
        Self {
            inner: Arc::new(MockInner {
                config,
                state: Mutex::new(State::default()),
                armed: Mutex::new(Armed::default()),
            }),
        }
    }

    pub fn config(&self) -> MockS3Config {
        self.inner.config.clone()
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.inner.state.lock().expect("mock s3 mutex poisoned")
    }

    fn armed(&self) -> std::sync::MutexGuard<'_, Armed> {
        self.inner.armed.lock().expect("mock s3 armed mutex poisoned")
    }

    /// Every object currently stored, keyed by object key.
    pub fn objects(&self) -> BTreeMap<String, Bytes> {
        self.state()
            .objects
            .iter()
            .map(|(key, object)| (key.clone(), object.bytes.clone()))
            .collect()
    }

    pub fn object(&self, key: &str) -> Option<Bytes> {
        self.state().objects.get(key).map(|object| object.bytes.clone())
    }

    pub fn object_count(&self) -> usize {
        self.state().objects.len()
    }

    pub fn events(&self) -> Vec<MockEvent> {
        self.state().events.clone()
    }

    /// Stable digest of the recorded event log (sequence order).
    pub fn event_digest(&self) -> u64 {
        let encoded = serde_json::to_vec(&self.events()).unwrap_or_default();
        digest_bytes(&[encoded])
    }

    /// Schedule-independent digest of the event log: events are sorted by
    /// `(key, op, attempt)` first, which is deterministic even when several
    /// callers race, then hashed.
    pub fn content_digest(&self) -> u64 {
        let mut events = self.events();
        events.sort_by(|a, b| {
            (&a.key, a.op as u8, a.attempt).cmp(&(&b.key, b.op as u8, b.attempt))
        });
        for event in &mut events {
            event.sequence = 0;
        }
        let encoded = serde_json::to_vec(&events).unwrap_or_default();
        digest_bytes(&[encoded])
    }

    pub fn successful_puts(&self) -> u64 {
        self.state().successful_puts
    }

    pub fn successful_cas(&self) -> u64 {
        self.state().successful_cas
    }

    /// Every successful PUT into the manifest object, in order, parsed.
    pub fn manifest_records(&self) -> Vec<ManifestRecord> {
        let raw = self.object(MANIFEST_KEY).unwrap_or_default();
        parse_manifest(&raw)
    }

    /// Fail every operation transiently for the next `ops` calls.
    pub fn begin_outage(&self, ops: u64) {
        self.state().outage_remaining = ops;
    }

    /// Start an outage of `ops` calls once `after_ops` operations have been
    /// recorded. Unlike [`MockS3::begin_outage`] this is armed before the run, so
    /// the scenario does not have to interleave itself into the uploader.
    pub fn begin_outage_after(&self, after_ops: u64, ops: u64) {
        self.armed().outage_after = Some((after_ops, ops));
    }

    pub fn outage_active(&self) -> bool {
        self.state().outage_remaining > 0
    }

    /// After the Nth successful PUT (1-based, counted across the whole run),
    /// report a fatal error even though the object landed.
    pub fn crash_after_successful_put(&self, index: u64) {
        self.armed().crash_after_successful_puts.insert(index);
    }

    /// After the Nth successful manifest CAS (1-based, counted across the whole
    /// run), report a fatal error even though the append landed.
    pub fn crash_after_successful_cas(&self, index: u64) {
        self.armed().crash_after_successful_cas.insert(index);
    }

    /// Once our PUT for `segment_id` lands, a competing writer appends a manifest
    /// record with `epoch` before our CAS can run. Our commit must observe
    /// [`bkxpro_uploader::CommitOutcome::Fenced`] and keep the local file.
    pub fn fence_after_put(&self, segment_id: u64, epoch: u64) {
        self.armed().fence_after_put.insert(segment_id, epoch);
    }

    fn decision_rng(&self, op: S3Op, key: &str, attempt: u64) -> DetRng {
        DetRng::new(self.inner.config.seed).derive_from(&[
            format!("{op:?}").as_bytes(),
            key.as_bytes(),
            &attempt.to_le_bytes(),
        ])
    }

    /// Allocate the next attempt and roll this operation's fate. Attempt
    /// allocation, outage consumption and the fault roll are all pure functions of
    /// the per-key attempt counter, so concurrent callers cannot change each
    /// other's outcome.
    fn begin_op(&self, op: S3Op, key: &str) -> (u64, u64, Decision) {
        let (attempt, in_outage) = {
            let mut state = self.state();
            // A pre-armed outage starts once enough operations have completed.
            {
                let mut armed = self.armed();
                if let Some((after_ops, ops)) = armed.outage_after {
                    if !armed.outage_fired && state.events.len() as u64 >= after_ops {
                        armed.outage_fired = true;
                        state.outage_remaining = ops;
                    }
                }
            }
            let counter = state.attempts.entry((op, key.to_string())).or_insert(0);
            *counter += 1;
            let attempt = *counter;
            let in_outage = if state.outage_remaining > 0 {
                state.outage_remaining -= 1;
                true
            } else {
                false
            };
            (attempt, in_outage)
        };
        if in_outage {
            return (attempt, 0, Decision::FailPreWrite(S3Outcome::Transient));
        }
        let config = &self.inner.config;
        let mut rng = self.decision_rng(op, key, attempt);
        // Always consume one draw for latency, even at zero jitter, so the
        // decision draw below is at a fixed position in the stream.
        let latency =
            config.latency_base_micros + rng.below(config.latency_jitter_micros + 1);
        let decision = match op {
            S3Op::Put => {
                if rng.chance(config.put_fail_percent) {
                    Decision::FailPreWrite(S3Outcome::Transient)
                } else if rng.chance(config.put_lost_response_percent) {
                    Decision::LostResponse
                } else {
                    Decision::Proceed
                }
            }
            S3Op::Cas => {
                if rng.chance(config.cas_conflict_percent) {
                    Decision::FailPreWrite(S3Outcome::Conflict)
                } else if rng.chance(config.cas_lost_response_percent) {
                    Decision::LostResponse
                } else {
                    Decision::Proceed
                }
            }
            S3Op::Get | S3Op::GetRange => {
                if rng.chance(config.get_fail_percent) {
                    Decision::FailPreWrite(S3Outcome::Transient)
                } else {
                    Decision::Proceed
                }
            }
            S3Op::Head => {
                if rng.chance(config.head_fail_percent) {
                    Decision::FailPreWrite(S3Outcome::Transient)
                } else {
                    Decision::Proceed
                }
            }
        };
        (attempt, latency, decision)
    }

    fn new_event(
        &self,
        op: S3Op,
        key: &str,
        attempt: u64,
        outcome: S3Outcome,
        bytes: u64,
        latency_micros: u64,
    ) -> MockEvent {
        MockEvent {
            sequence: 0,
            op,
            key: key.to_string(),
            attempt,
            outcome,
            bytes,
            latency_micros,
            segment_id: segment_id_of_key(key),
        }
    }

    /// Append one event, assigning its global sequence under the same lock.
    fn finish_event(&self, mut event: MockEvent) {
        let mut state = self.state();
        event.sequence = state.events.len() as u64;
        state.events.push(event);
    }

    async fn maybe_sleep(&self, latency_micros: u64) {
        if self.inner.config.real_latency && latency_micros > 0 {
            // Cap real sleeps so a chaos run cannot stall CI; the recorded latency
            // is the authoritative signal.
            let capped = latency_micros.min(50_000);
            tokio::time::sleep(std::time::Duration::from_micros(capped)).await;
        }
    }

    fn next_version(state: &mut State) -> (String, String) {
        state.version_counter += 1;
        let version = state.version_counter;
        (format!("etag-{version}"), format!("v{version}"))
    }

    /// Append a foreign manifest row (a competing writer winning the epoch race).
    fn append_foreign_manifest(&self, segment_id: u64, epoch: u64, key: &str) {
        let mut state = self.state();
        let mut raw = state
            .objects
            .get(MANIFEST_KEY)
            .map(|object| object.bytes.to_vec())
            .unwrap_or_default();
        let record = ManifestRecord {
            segment_id,
            key: key.to_string(),
            bytes: 0,
            crc32c: 0,
            writer_epoch: epoch,
            min_ledger: 0,
            max_ledger: 0,
            uploaded_at_ms: 0,
        };
        let mut row = serde_json::to_vec(&record).expect("manifest row encodes");
        row.push(b'\n');
        raw.extend_from_slice(&row);
        let (e_tag, version) = Self::next_version(&mut state);
        state.objects.insert(
            MANIFEST_KEY.to_string(),
            StoredObject {
                bytes: Bytes::from(raw),
                e_tag,
                version,
            },
        );
    }

    fn take_armed_after_put(&self, segment_id: u64) -> Option<u64> {
        self.armed().fence_after_put.remove(&segment_id)
    }

    fn crash_after_put(&self, successful_puts: u64) -> bool {
        self.armed()
            .crash_after_successful_puts
            .contains(&successful_puts)
    }

    fn crash_after_cas(&self, successful_cas: u64) -> bool {
        self.armed()
            .crash_after_successful_cas
            .contains(&successful_cas)
    }
}

/// Segment id parsed from a `segments/{id}.seg` key.
pub(crate) fn segment_id_of_key(key: &str) -> Option<u64> {
    let file = key.rsplit('/').next()?;
    let stem = file.strip_suffix(".seg")?;
    stem.parse::<u64>().ok()
}

fn object_version(object: &StoredObject) -> ObjectVersion {
    ObjectVersion {
        e_tag: Some(object.e_tag.clone()),
        version: Some(object.version.clone()),
    }
}

fn matches_version(object: &StoredObject, expected: &ObjectVersion) -> bool {
    let e_tag_matches = match (&expected.e_tag, &object.e_tag) {
        (Some(expected), actual) => expected == actual,
        (None, _) => true,
    };
    let version_matches = match (&expected.version, &object.version) {
        (Some(expected), actual) => expected == actual,
        (None, _) => true,
    };
    // Both fields are optional; a match on either is enough, which mirrors
    // `object_store`'s `PutMode::Update` semantics.
    (expected.e_tag.is_some() && e_tag_matches) || (expected.version.is_some() && version_matches)
}

async fn drain_stream(mut chunks: mpsc::Receiver<StreamItem>) -> Result<Vec<u8>, BlobError> {
    let mut bytes = Vec::new();
    while let Some(item) = chunks.recv().await {
        match item {
            StreamItem::Chunk(chunk) => bytes.extend_from_slice(&chunk),
            StreamItem::End { len, crc32c } => {
                if bytes.len() as u64 != len {
                    return Err(BlobError::Source(format!(
                        "stream ended after {} bytes but reported {len}",
                        bytes.len()
                    )));
                }
                let actual = crc32c::crc32c(&bytes);
                if actual != crc32c {
                    return Err(BlobError::Source(format!(
                        "stream checksum mismatch: got {actual:#010x}, expected {crc32c:#010x}"
                    )));
                }
                return Ok(bytes);
            }
            StreamItem::Failed(message) => {
                return Err(BlobError::Source(format!(
                    "reader failed during streamed upload: {message}"
                )))
            }
        }
    }
    Err(BlobError::Source(
        "stream closed without an end marker".to_string(),
    ))
}

#[async_trait]
impl BlobStore for MockS3 {
    async fn put_stream(
        &self,
        key: &str,
        _part_size: usize,
        chunks: mpsc::Receiver<StreamItem>,
    ) -> Result<PutOutcome, BlobError> {
        let (attempt, latency, decision) = self.begin_op(S3Op::Put, key);
        if let Decision::FailPreWrite(outcome) = decision {
            // Drop the receiver un-consumed: the reader observes a dead
            // connection, exactly like a failure before any part was accepted.
            drop(chunks);
            self.finish_event(self.new_event(S3Op::Put, key, attempt, outcome, 0, latency));
            return Err(BlobError::Transient(format!(
                "{key}: injected PUT failure (no bytes accepted)"
            )));
        }
        let lost = decision == Decision::LostResponse;
        let bytes = drain_stream(chunks).await?;
        self.maybe_sleep(latency).await;
        let len = bytes.len() as u64;
        // The outcome carries the checksum the *store* computed over the bytes it
        // accepted; the uploader verifies its reader's checksum against this.
        let crc = crc32c::crc32c(&bytes);
        let (e_tag, version, successful_puts) = {
            let mut state = self.state();
            let (e_tag, version) = Self::next_version(&mut state);
            state.objects.insert(
                key.to_string(),
                StoredObject {
                    bytes: Bytes::from(bytes),
                    e_tag: e_tag.clone(),
                    version: version.clone(),
                },
            );
            state.successful_puts += 1;
            (e_tag, version, state.successful_puts)
        };
        let outcome = if lost {
            S3Outcome::Transient
        } else {
            S3Outcome::Ok
        };
        self.finish_event(self.new_event(S3Op::Put, key, attempt, outcome, len, latency));

        // A competing writer may race our manifest commit once our object exists.
        if let Some(segment_id) = segment_id_of_key(key) {
            if let Some(epoch) = self.take_armed_after_put(segment_id) {
                self.append_foreign_manifest(segment_id, epoch, key);
            }
        }

        if self.crash_after_put(successful_puts) {
            return Err(BlobError::Fatal(
                "simulated process death after a successful PUT".to_string(),
            ));
        }
        if lost {
            return Err(BlobError::Transient(format!(
                "{key}: injected lost PUT response"
            )));
        }
        Ok(PutOutcome {
            len,
            crc32c: crc,
            version: ObjectVersion {
                e_tag: Some(e_tag),
                version: Some(version),
            },
        })
    }

    async fn get(&self, key: &str) -> Result<BlobObject, BlobError> {
        let (attempt, latency, decision) = self.begin_op(S3Op::Get, key);
        self.maybe_sleep(latency).await;
        if let Decision::FailPreWrite(outcome) = decision {
            self.finish_event(self.new_event(S3Op::Get, key, attempt, outcome, 0, latency));
            return Err(BlobError::Transient(format!(
                "{key}: injected GET failure"
            )));
        }
        let object = self.state().objects.get(key).cloned();
        match object {
            Some(object) => {
                let info = ObjectInfo {
                    key: key.to_string(),
                    len: object.bytes.len() as u64,
                    version: object_version(&object),
                };
                self.finish_event(self.new_event(
                    S3Op::Get,
                    key,
                    attempt,
                    S3Outcome::Ok,
                    info.len,
                    latency,
                ));
                Ok(BlobObject {
                    bytes: object.bytes,
                    info,
                })
            }
            None => {
                self.finish_event(self.new_event(
                    S3Op::Get,
                    key,
                    attempt,
                    S3Outcome::NotFound,
                    0,
                    latency,
                ));
                Err(BlobError::NotFound(key.to_string()))
            }
        }
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, BlobError> {
        let (attempt, latency, decision) = self.begin_op(S3Op::GetRange, key);
        self.maybe_sleep(latency).await;
        if let Decision::FailPreWrite(outcome) = decision {
            self.finish_event(self.new_event(
                S3Op::GetRange,
                key,
                attempt,
                outcome,
                0,
                latency,
            ));
            return Err(BlobError::Transient(format!(
                "{key}: injected ranged GET failure"
            )));
        }
        let object = self.state().objects.get(key).cloned();
        let Some(object) = object else {
            self.finish_event(self.new_event(
                S3Op::GetRange,
                key,
                attempt,
                S3Outcome::NotFound,
                0,
                latency,
            ));
            return Err(BlobError::NotFound(key.to_string()));
        };
        let len = object.bytes.len() as u64;
        if range.start > range.end || range.end > len {
            return Err(BlobError::Invalid(format!(
                "{key}: range {}..{} is outside the {len}-byte object",
                range.start, range.end
            )));
        }
        let slice = object.bytes.slice(range.start as usize..range.end as usize);
        self.finish_event(self.new_event(
            S3Op::GetRange,
            key,
            attempt,
            S3Outcome::Ok,
            slice.len() as u64,
            latency,
        ));
        Ok(slice)
    }

    async fn head(&self, key: &str) -> Result<ObjectInfo, BlobError> {
        let (attempt, latency, decision) = self.begin_op(S3Op::Head, key);
        self.maybe_sleep(latency).await;
        if let Decision::FailPreWrite(outcome) = decision {
            self.finish_event(self.new_event(S3Op::Head, key, attempt, outcome, 0, latency));
            return Err(BlobError::Transient(format!(
                "{key}: injected HEAD failure"
            )));
        }
        let object = self.state().objects.get(key).cloned();
        match object {
            Some(object) => {
                let info = ObjectInfo {
                    key: key.to_string(),
                    len: object.bytes.len() as u64,
                    version: object_version(&object),
                };
                self.finish_event(self.new_event(
                    S3Op::Head,
                    key,
                    attempt,
                    S3Outcome::Ok,
                    info.len,
                    latency,
                ));
                Ok(info)
            }
            None => {
                self.finish_event(self.new_event(
                    S3Op::Head,
                    key,
                    attempt,
                    S3Outcome::NotFound,
                    0,
                    latency,
                ));
                Err(BlobError::NotFound(key.to_string()))
            }
        }
    }

    async fn put_if_version(
        &self,
        key: &str,
        data: Bytes,
        expected: ExpectedVersion,
    ) -> Result<ObjectInfo, BlobError> {
        let (attempt, latency, decision) = self.begin_op(S3Op::Cas, key);
        if let Decision::FailPreWrite(outcome) = decision {
            let error = match outcome {
                S3Outcome::Conflict => {
                    BlobError::Conflict(format!("{key}: injected CAS conflict"))
                }
                _ => BlobError::Transient(format!("{key}: injected transient CAS failure")),
            };
            self.finish_event(self.new_event(S3Op::Cas, key, attempt, outcome, 0, latency));
            return Err(error);
        }
        let lost = decision == Decision::LostResponse;
        self.maybe_sleep(latency).await;
        let len = data.len() as u64;
        let (info, successful_cas) = {
            let mut state = self.state();
            match state.objects.get(key) {
                Some(existing) => match &expected {
                    ExpectedVersion::MustNotExist => {
                        drop(state);
                        self.finish_event(self.new_event(
                            S3Op::Cas,
                            key,
                            attempt,
                            S3Outcome::Conflict,
                            0,
                            latency,
                        ));
                        return Err(BlobError::Conflict(format!(
                            "{key}: object already exists"
                        )));
                    }
                    ExpectedVersion::Match(version) => {
                        if !matches_version(existing, version) {
                            drop(state);
                            self.finish_event(self.new_event(
                                S3Op::Cas,
                                key,
                                attempt,
                                S3Outcome::Conflict,
                                0,
                                latency,
                            ));
                            return Err(BlobError::Conflict(format!(
                                "{key}: version no longer current"
                            )));
                        }
                    }
                },
                None => {
                    if matches!(expected, ExpectedVersion::Match(_)) {
                        drop(state);
                        self.finish_event(self.new_event(
                            S3Op::Cas,
                            key,
                            attempt,
                            S3Outcome::Conflict,
                            0,
                            latency,
                        ));
                        return Err(BlobError::Conflict(format!(
                            "{key}: object disappeared"
                        )));
                    }
                }
            }
            let (e_tag, version) = Self::next_version(&mut state);
            state.objects.insert(
                key.to_string(),
                StoredObject {
                    bytes: data,
                    e_tag: e_tag.clone(),
                    version: version.clone(),
                },
            );
            state.successful_cas += 1;
            let info = ObjectInfo {
                key: key.to_string(),
                len,
                version: ObjectVersion {
                    e_tag: Some(e_tag),
                    version: Some(version),
                },
            };
            (info, state.successful_cas)
        };
        let outcome = if lost {
            S3Outcome::Transient
        } else {
            S3Outcome::Ok
        };
        if self.crash_after_cas(successful_cas) {
            self.finish_event(self.new_event(
                S3Op::Cas,
                key,
                attempt,
                S3Outcome::Fatal,
                len,
                latency,
            ));
            return Err(BlobError::Fatal(
                "simulated process death after a successful manifest CAS".to_string(),
            ));
        }
        self.finish_event(self.new_event(S3Op::Cas, key, attempt, outcome, len, latency));
        if lost {
            return Err(BlobError::Transient(format!(
                "{key}: injected lost CAS response"
            )));
        }
        Ok(info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn put_payload(
        store: &MockS3,
        key: &str,
        payload: &[u8],
    ) -> Result<PutOutcome, BlobError> {
        let (tx, rx) = mpsc::channel(4);
        let (len, crc) = (payload.len() as u64, crc32c::crc32c(payload));
        let payload = payload.to_vec();
        let feeder = tokio::spawn(async move {
            for chunk in payload.chunks(7) {
                if tx
                    .send(StreamItem::Chunk(Bytes::copy_from_slice(chunk)))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            let _ = tx.send(StreamItem::End { len, crc32c: crc }).await;
        });
        let outcome = store.put_stream(key, 4096, rx).await;
        let _ = feeder.await;
        outcome
    }

    #[tokio::test]
    async fn content_digest_ignores_cross_key_event_order() {
        let first = MockS3::new(1);
        let second = MockS3::new(1);
        for key in [segment_key(1), segment_key(2)] {
            put_payload(&first, &key, b"payload").await.unwrap();
        }
        for key in [segment_key(2), segment_key(1)] {
            put_payload(&second, &key, b"payload").await.unwrap();
        }
        assert_eq!(first.content_digest(), second.content_digest());
        assert_ne!(first.event_digest(), second.event_digest());
        assert_eq!(
            first.events()[1].sequence,
            1,
            "digest must not mutate the log"
        );
        put_payload(&second, &segment_key(3), b"payload")
            .await
            .unwrap();
        assert_ne!(first.content_digest(), second.content_digest());
    }

    #[tokio::test]
    async fn clean_put_round_trips_bytes_and_checksum() {
        let store = MockS3::new(1);
        let payload: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
        let outcome = put_payload(&store, &segment_key(7), &payload).await.unwrap();
        assert_eq!(outcome.len, payload.len() as u64);
        assert_eq!(outcome.crc32c, crc32c::crc32c(&payload));
        assert_eq!(store.object(&segment_key(7)).unwrap().as_ref(), payload);
        assert_eq!(store.events().len(), 1);
        assert_eq!(store.events()[0].segment_id, Some(7));
    }

    #[tokio::test]
    async fn lost_response_lands_the_object_but_reports_an_error() {
        let config = MockS3Config {
            put_lost_response_percent: 100,
            ..MockS3Config::default()
        };
        let store = MockS3::with_config(config);
        let error = put_payload(&store, &segment_key(1), b"payload")
            .await
            .unwrap_err();
        assert!(matches!(error, BlobError::Transient(_)));
        assert_eq!(store.object(&segment_key(1)).unwrap().as_ref(), b"payload");
    }

    #[tokio::test]
    async fn pre_write_failure_never_stores_anything() {
        let config = MockS3Config {
            put_fail_percent: 100,
            ..MockS3Config::default()
        };
        let store = MockS3::with_config(config);
        let error = put_payload(&store, &segment_key(1), b"payload")
            .await
            .unwrap_err();
        assert!(matches!(error, BlobError::Transient(_)));
        assert_eq!(store.object_count(), 0);
    }

    #[tokio::test]
    async fn cas_conflict_and_lost_response_are_distinct() {
        let config = MockS3Config {
            cas_conflict_percent: 100,
            ..MockS3Config::default()
        };
        let store = MockS3::with_config(config);
        let error = store
            .put_if_version(
                MANIFEST_KEY,
                Bytes::from_static(b"row\n"),
                ExpectedVersion::MustNotExist,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, BlobError::Conflict(_)));
        assert_eq!(store.object_count(), 0);

        let config = MockS3Config {
            cas_lost_response_percent: 100,
            ..MockS3Config::default()
        };
        let store = MockS3::with_config(config);
        let error = store
            .put_if_version(
                MANIFEST_KEY,
                Bytes::from_static(b"row\n"),
                ExpectedVersion::MustNotExist,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, BlobError::Transient(_)));
        assert_eq!(store.object(MANIFEST_KEY).unwrap().as_ref(), b"row\n");
    }

    #[tokio::test]
    async fn crash_points_fire_on_success_counts() {
        let store = MockS3::new(3);
        store.crash_after_successful_put(2);
        put_payload(&store, &segment_key(1), b"a").await.unwrap();
        let error = put_payload(&store, &segment_key(2), b"b").await.unwrap_err();
        assert!(matches!(error, BlobError::Fatal(_)));
        assert_eq!(store.object(&segment_key(2)).unwrap().as_ref(), b"b");
    }

    #[tokio::test]
    async fn outage_after_fires_once_at_the_threshold() {
        let store = MockS3::new(3);
        store.begin_outage_after(1, 2);
        // The first operation is recorded; the outage starts when the next
        // operation begins, then covers it and one more call.
        let first = store.head(&segment_key(1)).await;
        assert!(matches!(first, Err(BlobError::NotFound(_))));
        for _ in 0..2 {
            let error = store.head(&segment_key(1)).await.unwrap_err();
            assert!(matches!(error, BlobError::Transient(_)));
        }
        assert!(!store.outage_active());
        // The window is exhausted; the store answers normally again.
        let after = store.head(&segment_key(1)).await;
        assert!(matches!(after, Err(BlobError::NotFound(_))));
        assert_eq!(store.events().len(), 4);
    }

    #[tokio::test]
    async fn fences_append_a_newer_manifest_epoch_after_put() {
        let store = MockS3::new(4);
        store.fence_after_put(5, 9);
        put_payload(&store, &segment_key(5), b"x").await.unwrap();
        let records = store.manifest_records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].segment_id, 5);
        assert_eq!(records[0].writer_epoch, 9);
    }

    #[tokio::test]
    async fn outage_fails_everything_transiently_for_its_window() {
        let store = MockS3::new(5);
        store.begin_outage(2);
        let error = put_payload(&store, &segment_key(1), b"a").await.unwrap_err();
        assert!(matches!(error, BlobError::Transient(_)));
        let error = store
            .put_if_version(
                MANIFEST_KEY,
                Bytes::from_static(b"row\n"),
                ExpectedVersion::MustNotExist,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, BlobError::Transient(_)));
        assert!(!store.outage_active());
        assert!(put_payload(&store, &segment_key(1), b"a").await.is_ok());
    }

    #[tokio::test]
    async fn a_clone_shares_counters_and_arming() {
        let store = MockS3::new(6);
        let clone = store.clone();
        clone.crash_after_successful_put(2);
        put_payload(&store, &segment_key(1), b"a").await.unwrap();
        // The clone observes the same successful-PUT counter, so this PUT trips
        // the crash point armed through the other handle.
        let error = put_payload(&store, &segment_key(2), b"b").await.unwrap_err();
        assert!(matches!(error, BlobError::Fatal(_)));
    }

    #[test]
    fn segment_keys_round_trip_through_the_parser() {
        assert_eq!(segment_id_of_key(&segment_key(42)), Some(42));
        assert_eq!(segment_id_of_key(MANIFEST_KEY), None);
    }

    #[test]
    fn decisions_are_reproducible_for_the_same_seed() {
        let config = MockS3Config {
            seed: 99,
            put_fail_percent: 30,
            put_lost_response_percent: 30,
            cas_conflict_percent: 30,
            cas_lost_response_percent: 30,
            get_fail_percent: 30,
            head_fail_percent: 30,
            ..MockS3Config::default()
        };
        let first = MockS3::with_config(config.clone());
        let second = MockS3::with_config(config);
        for attempt in 1..50 {
            let a = first.begin_op(S3Op::Put, "segments/1.seg");
            let b = second.begin_op(S3Op::Put, "segments/1.seg");
            assert_eq!(a, b, "attempt {attempt}");
            let a = first.begin_op(S3Op::Cas, MANIFEST_KEY);
            let b = second.begin_op(S3Op::Cas, MANIFEST_KEY);
            assert_eq!(a, b, "attempt {attempt}");
        }
    }
}
