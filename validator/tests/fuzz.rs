//! Integration tests for the structural fuzzer.
//!
//! Every test is a pure function of fixed seeds, so failures always reproduce;
//! the nightly job adds random seeds through the CLI (`bkxpro-validator fuzz`).

use std::{fs, path::Path};

use bkxpro_validator::fuzz::{audit_image, run_fuzz, run_tail_fuzz, write_corpus, FuzzConfig};

fn corpus_path() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../codec/fuzz/corpus/recovery_parser")
}

#[test]
fn structural_mutations_never_break_recovery_prefixes() {
    for seed in [1u64, 7, 424_242] {
        let config = FuzzConfig {
            seed,
            iterations: 500,
            ..FuzzConfig::default()
        };
        let report = run_fuzz(&config);
        assert!(
            report.ok(),
            "seed {seed} produced violations: {:#?}",
            report.violations
        );
        assert!(
            report.recovered_views > 0,
            "seed {seed} never produced a recovery view; the fuzzer is not exercising recovery"
        );
        assert!(
            report.invalid_headers > 0,
            "seed {seed} never produced an invalid header"
        );
        assert!(
            report.sealed_parses < report.iterations,
            "sealed parsing accepted every mutation; the checks are not sensitive"
        );
    }
}

#[test]
fn tail_faults_recover_exactly_the_surviving_prefix() {
    for seed in [2u64, 11, 99] {
        let config = FuzzConfig {
            seed,
            iterations: 500,
            ..FuzzConfig::default()
        };
        let report = run_tail_fuzz(&config);
        assert!(
            report.ok(),
            "seed {seed} produced violations: {:#?}",
            report.violations
        );
        assert!(report.recovered_records > 0);
        assert!(
            report.invalid_headers > 0,
            "seed {seed}: no truncation ever destroyed the header"
        );
    }
}

#[test]
fn multi_block_records_are_exercised() {
    // 12 KiB payloads need three blocks each; the prefix and exact-count
    // properties must hold across block boundaries too.
    let config = FuzzConfig {
        seed: 3,
        iterations: 300,
        entries: 6,
        payload_bytes: 12 * 1024,
        max_mutations: 3,
    };
    let report = run_fuzz(&config);
    assert!(report.ok(), "{:#?}", report.violations);
    assert!(report.recovered_records > 0);
    let tail = run_tail_fuzz(&config);
    assert!(tail.ok(), "{:#?}", tail.violations);
    assert!(tail.recovered_records > 0);
}

#[test]
fn corpus_is_reproducible_and_audits_clean() {
    let dir = tempfile::TempDir::new().unwrap();
    let first = dir.path().join("first");
    let second = dir.path().join("second");
    let count = write_corpus(&first, 9).unwrap();
    assert_eq!(count, write_corpus(&second, 9).unwrap());
    assert!(count >= 24);

    // Byte-for-byte reproducibility: the checked-in corpus can be regenerated.
    let mut compared = 0usize;
    let mut stack = vec![first.clone()];
    while let Some(next) = stack.pop() {
        for entry in fs::read_dir(&next).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let relative = path.strip_prefix(&first).unwrap();
            let mirrored = second.join(relative);
            assert_eq!(
                fs::read(&path).unwrap(),
                fs::read(&mirrored).unwrap(),
                "{relative:?} is not reproducible"
            );
            compared += 1;
        }
    }
    assert_eq!(compared, count);

    // Every recovery image must pass the same audit the fuzzer applies.
    for entry in fs::read_dir(first.join("recovery_parser")).unwrap() {
        let path = entry.unwrap().path();
        let bytes = fs::read(&path).unwrap();
        let violations = audit_image(9, &bytes);
        assert!(violations.is_empty(), "{}: {violations:?}", path.display());
    }
}

#[test]
fn checked_in_corpus_audits_clean() {
    let corpus = corpus_path();
    assert!(
        corpus.is_dir(),
        "checked-in corpus missing at {}; regenerate with \
         `cargo run -p bkxpro-validator --bin bkxpro-validator -- fuzz --seed 9 --write-corpus codec/fuzz/corpus`",
        corpus.display()
    );
    let mut count = 0usize;
    for entry in fs::read_dir(&corpus).unwrap() {
        let path = entry.unwrap().path();
        let bytes = fs::read(&path).unwrap();
        let violations = audit_image(9, &bytes);
        assert!(violations.is_empty(), "{}: {violations:?}", path.display());
        count += 1;
    }
    assert!(count >= 24, "corpus shrank to {count} files");
}
