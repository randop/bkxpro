//! Seeded scenario plans.
//!
//! A [`Plan`] is a pure function of `(scenario name, seed)`: every knob a run
//! needs - fault rates, crash points, work volume, uploader tuning - is derived
//! from the seed, so `(name, seed)` fully determines a replay. The presets below
//! are the CI matrix; `--seed N` with a random seed explores the same family.
//!
//! Scenarios are grouped by what they are meant to break:
//!
//! | preset            | hazard under test                                            |
//! |-------------------|--------------------------------------------------------------|
//! | `smoke`           | happy path: write, seal, upload, delete, read back           |
//! | `crash-upload`    | process death between PUT/HEAD/CAS/delete                    |
//! | `crash-lifecycle` | process death mid-append and mid-seal; index rebuild          |
//! | `power-cut`       | power loss: unsynced/torn tail, open-segment recovery         |
//! | `disk-faults`     | EIO/EAGAIN/ENOSPC/corruption on writes, syncs, truncates      |
//! | `fenced`          | competing writer wins the manifest epoch race                 |
//! | `s3-chaos`        | lost responses, CAS races, GET/HEAD failures, latency spikes  |
//! | `outage`          | an S3 outage window that fails everything transiently         |
//! | `slow-sync`       | slow device syncs under concurrent uploads                    |

use serde::{Deserialize, Serialize};

use crate::mock_s3::MockS3Config;
use crate::rng::{fnv1a, DetRng};

/// Byte size of one codec block; plans never emit sizes that break alignment.
pub const BLOCK: usize = 4096;

/// The default scenario matrix, in CI order. Deterministic presets run on every
/// PR with fixed seeds; nightly runs add random seeds on top.
pub const PRESETS: &[&str] = &[
    "smoke",
    "crash-upload",
    "crash-lifecycle",
    "power-cut",
    "disk-faults",
    "fenced",
    "s3-chaos",
    "outage",
    "slow-sync",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ScenarioKind {
    Smoke,
    CrashUpload,
    CrashLifecycle,
    PowerCut,
    DiskFaults,
    Fenced,
    S3Chaos,
    Outage,
    SlowSync,
}

impl ScenarioKind {
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "smoke" => Self::Smoke,
            "crash-upload" => Self::CrashUpload,
            "crash-lifecycle" => Self::CrashLifecycle,
            "power-cut" => Self::PowerCut,
            "disk-faults" => Self::DiskFaults,
            "fenced" => Self::Fenced,
            "s3-chaos" => Self::S3Chaos,
            "outage" => Self::Outage,
            "slow-sync" => Self::SlowSync,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::CrashUpload => "crash-upload",
            Self::CrashLifecycle => "crash-lifecycle",
            Self::PowerCut => "power-cut",
            Self::DiskFaults => "disk-faults",
            Self::Fenced => "fenced",
            Self::S3Chaos => "s3-chaos",
            Self::Outage => "outage",
            Self::SlowSync => "slow-sync",
        }
    }
}

/// A deterministic disk fault a scenario can arm at a chosen append.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "fault", rename_all = "snake_case")]
pub enum DiskFault {
    FailWrite,
    Eagain,
    Enospc,
    /// Short write: `prefix_bytes` land, the next bytes become garbage.
    Torn {
        prefix_bytes: usize,
        garbage_bytes: usize,
    },
    /// The write succeeds, then one byte is flipped.
    Corrupt {
        byte_offset: usize,
        xor: u8,
    },
    FailSync,
    SlowSync {
        micros: u64,
    },
    FailTruncate,
    FailDirectorySync,
    FailAcquire,
    /// The next read observes the last durable bytes.
    StaleRead,
}

/// One fault arming instruction: arm `fault` right before the entry at
/// `at_append` (0-based index within the segment's entry list) is appended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FaultArm {
    pub at_append: usize,
    pub fault: DiskFault,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CrashPlan {
    /// Report process death after the Nth successful PUT (1-based).
    pub after_successful_puts: Option<u64>,
    /// Report process death after the Nth successful manifest CAS (1-based).
    pub after_successful_cas: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FencePlan {
    /// 0-based index of the segment to fence.
    pub segment_index: u64,
    /// Epoch the competing writer commits before our CAS.
    pub epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutagePlan {
    /// Start the outage once this many mock-S3 operations completed.
    pub after_ops: u64,
    /// Ops that fail transiently while the outage is active.
    pub ops: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkPlan {
    /// Number of segments the writer produces.
    pub segments: u64,
    /// Entries per segment.
    pub entries_per_segment: u64,
    /// Ledgers spread across entries (`ledger = entry % ledgers + 1`).
    pub ledgers: u64,
    /// Payload byte length for every entry.
    pub payload_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterPlan {
    pub target_data_bytes: usize,
    pub max_entries: usize,
    pub group_commit_bytes: usize,
    pub max_inflight_bytes: usize,
    pub prezero_batch_blocks: usize,
    /// Entries appended per segment before sealing in the happy path.
    pub seal_at_entries: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploaderPlan {
    pub part_size: usize,
    pub read_chunk_bytes: usize,
    pub max_inflight_bytes: usize,
    pub stream_channel_capacity: usize,
    pub max_retries: u32,
    pub retry_base_ms: u64,
    pub retry_max_ms: u64,
    pub retry_timeout_ms: u64,
}

/// One seeded scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Plan {
    pub name: String,
    pub seed: u64,
    pub kind: ScenarioKind,
    pub mock: MockS3Config,
    pub work: WorkPlan,
    pub writer: WriterPlan,
    pub uploader: UploaderPlan,
    pub crash: Option<CrashPlan>,
    pub fence: Option<FencePlan>,
    pub outage: Option<OutagePlan>,
    /// Disk faults armed during the first segment's write.
    pub disk_faults: Vec<FaultArm>,
    /// Stop the first segment's append loop after this many entries (0 = never).
    pub stop_first_segment_after: usize,
    /// Crash the simulated device after the first write phase (power loss).
    pub power_cut_after_write: bool,
}

impl Plan {
    /// Stable, human-readable fingerprint of the plan; printed with every report
    /// so an incident can cite one value.
    pub fn fingerprint(&self) -> u64 {
        let encoded = serde_json::to_vec(self).unwrap_or_default();
        crate::rng::digest_bytes(&[encoded])
    }

    /// Short replay command an operator can paste.
    pub fn replay_command(&self) -> String {
        format!(
            "cargo run -p bkxpro-validator --bin bkxpro-validator -- sim --scenario {} --seed {}",
            self.kind.name(),
            self.seed
        )
    }
}

/// Derive the plan for a preset and seed. Returns `None` for unknown scenarios.
pub fn preset(name: &str, seed: u64) -> Option<Plan> {
    let kind = ScenarioKind::parse(name)?;
    let mut rng = DetRng::new(seed ^ fnv1a(name.as_bytes()));

    let mut plan = Plan {
        name: name.to_string(),
        seed,
        kind,
        mock: MockS3Config {
            seed,
            ..MockS3Config::default()
        },
        work: WorkPlan {
            segments: 3,
            entries_per_segment: 40,
            ledgers: 2,
            payload_bytes: 96,
        },
        writer: WriterPlan {
            target_data_bytes: 64 * 1024,
            max_entries: 256,
            group_commit_bytes: 4 * BLOCK,
            max_inflight_bytes: 4 * 1024 * 1024,
            prezero_batch_blocks: 64,
            seal_at_entries: 40,
        },
        uploader: UploaderPlan {
            part_size: 64 * 1024,
            read_chunk_bytes: 4 * BLOCK,
            max_inflight_bytes: 256 * 1024,
            stream_channel_capacity: 4,
            max_retries: 6,
            retry_base_ms: 1,
            retry_max_ms: 8,
            retry_timeout_ms: 30_000,
        },
        crash: None,
        fence: None,
        outage: None,
        disk_faults: Vec::new(),
        stop_first_segment_after: 0,
        power_cut_after_write: false,
    };

    match kind {
        ScenarioKind::Smoke => {}
        ScenarioKind::CrashUpload => {
            plan.work.segments = 4;
            plan.crash = Some(CrashPlan {
                // Crash in the middle of the upload pipeline; the exact
                // successful-PUT ordinal is randomized by the seed.
                after_successful_puts: Some(rng.range_inclusive(1, 2)),
                after_successful_cas: Some(rng.range_inclusive(1, 1)),
            });
        }
        ScenarioKind::CrashLifecycle => {
            plan.work.segments = 4;
            // 1 KiB records so group commits actually acknowledge entries before
            // the crash; with 96-byte records nothing commits before the seal.
            plan.work.payload_bytes = 1024;
            // Stop mid-append: the segment stays open and must be recovered.
            plan.stop_first_segment_after = rng.range_inclusive(5, 20) as usize;
            plan.power_cut_after_write = true;
        }
        ScenarioKind::PowerCut => {
            plan.work.segments = 3;
            plan.work.payload_bytes = 1024;
            plan.stop_first_segment_after = rng.range_inclusive(6, 24) as usize;
            plan.power_cut_after_write = true;
            // A torn completion plus a lost sync is the classic power-loss shape.
            let at = plan.stop_first_segment_after.saturating_sub(1);
            plan.disk_faults = vec![FaultArm {
                at_append: at,
                fault: DiskFault::Torn {
                    prefix_bytes: BLOCK,
                    garbage_bytes: 512,
                },
            }];
        }
        ScenarioKind::DiskFaults => {
            plan.work.segments = 3;
            // 3 KiB records: every group commit covers a handful of entries, so
            // faults land both before and after acknowledgements.
            plan.work.entries_per_segment = 12;
            plan.work.payload_bytes = 3072;
            let at = rng.range_inclusive(2, 10) as usize;
            let fault = match rng.below(4) {
                0 => DiskFault::FailWrite,
                1 => DiskFault::Eagain,
                2 => DiskFault::Enospc,
                _ => DiskFault::FailSync,
            };
            plan.disk_faults = vec![FaultArm {
                at_append: at,
                fault,
            }];
            plan.power_cut_after_write = true;
        }
        ScenarioKind::Fenced => {
            plan.work.segments = 3;
            plan.fence = Some(FencePlan {
                segment_index: rng.range_inclusive(0, 1),
                // Any epoch above the writer's own epoch fences it out.
                epoch: 1_000_000 + rng.below(1000),
            });
        }
        ScenarioKind::S3Chaos => {
            plan.work.segments = 4;
            plan.work.entries_per_segment = 60;
            plan.mock.put_fail_percent = 15;
            plan.mock.put_lost_response_percent = 12;
            plan.mock.cas_conflict_percent = 12;
            plan.mock.cas_lost_response_percent = 8;
            plan.mock.get_fail_percent = 6;
            plan.mock.head_fail_percent = 6;
            plan.mock.latency_base_micros = 50;
            plan.mock.latency_jitter_micros = 500;
        }
        ScenarioKind::Outage => {
            plan.work.segments = 4;
            plan.outage = Some(OutagePlan {
                after_ops: rng.range_inclusive(2, 6),
                ops: rng.range_inclusive(4, 12),
            });
        }
        ScenarioKind::SlowSync => {
            plan.work.segments = 3;
            plan.disk_faults = vec![
                FaultArm {
                    at_append: 4,
                    fault: DiskFault::SlowSync {
                        micros: rng.range_inclusive(500, 5_000),
                    },
                },
                FaultArm {
                    at_append: 12,
                    fault: DiskFault::SlowSync {
                        micros: rng.range_inclusive(500, 5_000),
                    },
                },
            ];
            plan.mock.latency_base_micros = 20;
            plan.mock.latency_jitter_micros = 200;
        }
    }

    Some(plan)
}

/// Every preset at one seed; used by the CI matrix and the CLI `--all` flag.
pub fn all_presets(seed: u64) -> Vec<Plan> {
    PRESETS
        .iter()
        .filter_map(|name| preset(name, seed))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presets_are_reproducible() {
        for name in PRESETS {
            let first = preset(name, 7).unwrap();
            let second = preset(name, 7).unwrap();
            assert_eq!(first, second);
            assert_eq!(first.fingerprint(), second.fingerprint());
            assert_eq!(first.name, *name);
        }
    }

    #[test]
    fn seeds_change_the_derived_plan() {
        // At least one preset must react to the seed, otherwise nightly random
        // seeds would be pointless.
        let changed = PRESETS
            .iter()
            .filter(|name| preset(name, 1).unwrap() != preset(name, 2).unwrap())
            .count();
        assert!(changed >= 3, "only {changed} presets vary with the seed");
    }

    #[test]
    fn scenario_kinds_round_trip_through_their_names() {
        for name in PRESETS {
            let kind = ScenarioKind::parse(name).unwrap();
            assert_eq!(kind.name(), *name);
            assert!(preset(name, 0).is_some());
        }
        assert!(preset("no-such-scenario", 0).is_none());
    }

    #[test]
    fn replay_commands_are_copy_pasteable() {
        let plan = preset("s3-chaos", 42).unwrap();
        assert!(plan
            .replay_command()
            .contains("--scenario s3-chaos --seed 42"));
    }
}
