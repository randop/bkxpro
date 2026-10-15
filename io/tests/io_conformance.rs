use bkxpro_io::backend::{IoUringBackend, SimulatedBackend};
use bkxpro_io::io_trait::StorageIo;
use tempfile::tempdir;

fn test_storage_conformance(backend: impl StorageIo) {
    let dir = tempdir().unwrap();
    let segment_path = dir.path().join("segment_00001.seg");

    // 1. Preallocate and pre-zero
    backend
        .preallocate_and_prezero(&segment_path, 8192)
        .unwrap();

    // 2. Open segment and write aligned blocks
    {
        let mut file = backend.open_segment(&segment_path, false).unwrap();
        let buf = vec![0xAB; 4096];
        let n = file.aligned_write(0, &buf).unwrap();
        assert_eq!(n, 4096);
        file.datasync().unwrap();
    }

    // 3. Read back and verify
    {
        let mut file = backend.open_segment(&segment_path, false).unwrap();
        let mut read_buf = vec![0u8; 4096];
        let n = file.aligned_read(0, &mut read_buf).unwrap();
        assert_eq!(n, 4096);
        assert_eq!(read_buf[0], 0xAB);
        assert_eq!(file.len().unwrap(), 8192);
    }

    // 4. Truncate test
    {
        let mut file = backend.open_segment(&segment_path, false).unwrap();
        file.truncate(4096).unwrap();
        assert_eq!(file.len().unwrap(), 4096);
    }

    // 5. Unlink and sync dir
    backend.unlink_and_sync_dir(&segment_path).unwrap();
    assert!(!segment_path.exists());
}

#[test]
fn test_simulated_conformance() {
    let backend = SimulatedBackend::new();
    test_storage_conformance(backend);
}

#[test]
#[cfg(target_os = "linux")]
fn test_iouring_conformance() {
    if std::env::var("CI_RUN_IO_URING").is_ok() {
        let backend = IoUringBackend::new(256).unwrap();
        test_storage_conformance(backend);
    }
}
