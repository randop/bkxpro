//! Validator harness for bkxpro (RAN-132).
//!
//! The harness drives the real segment-writer, index, and uploader code paths
//! against a deterministic simulated storage backend (RAN-128/RAN-134) and a
//! fault-injecting S3 stand-in. Every fault decision derives from the seed and the
//! content it applies to, so a failing run can always be replayed:
//!
//! ```text
//! cargo run -p bkxpro-validator --bin bkxpro-validator -- sim --scenario s3-chaos --seed 42 --replay
//! ```
//!
//! Modules:
//!
//! * [`rng`] - SplitMix64 with content-keyed stream derivation.
//! * [`trace`] - append-only event trace with stable digests and invariant checks.
//! * [`sim_io`] - shared handle over the deterministic `SimulatedSegmentIo` backend.
//! * [`mock_s3`] - seeded S3 stand-in that models lost responses, CAS races,
//!   fencing, outages, and crash points.

pub mod fuzz;
pub mod kill_soak;
pub mod lifecycle;
pub mod mem;
pub mod mock_s3;
pub mod plan;
pub mod plain_io;
pub mod real_s3;
pub mod report;
pub mod rng;
pub mod scenarios;
pub mod sim_io;
pub mod soak;
pub mod trace;

pub use kill_soak::{
    read_child_meta, run_kill_soak, verify_session_dir, ChildConfig, ChildMeta, KillRound,
    KillSoakConfig, KillSoakOutcome, JournalEntry, SessionVerifyReport,
};
pub use plain_io::PlainFileIo;
pub use real_s3::{run_real_s3, RealS3Config, RealS3Report};
pub use soak::{run_soak, SoakConfig, SoakReport};

pub use lifecycle::{HarnessError, Session, WriteEnd, WriteOutcome, WriteSpec};
pub use mock_s3::{MockS3, MockS3Config, S3Op, S3Outcome};
pub use plan::{preset, Plan, ScenarioKind, PRESETS};
pub use rng::DetRng;
pub use scenarios::{run_plan, RunReport};
pub use sim_io::SharedSimIo;
pub use trace::{Trace, TraceEvent};
