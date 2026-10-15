use bkxpro_io::backend::SimulatedBackend;
use bkxpro_io::io_trait::StorageIo;
use tempfile::tempdir;

#[test]
fn test_simulated_unaligned_write_error() {
    let backend = SimulatedBackend::new();
    let dir = tempdir().unwrap();
    let path = dir.path().join("unaligned.seg");

    backend.preallocate_and_prezero(&path, 8192).unwrap();
    let mut file = backend.open_segment(&path, false).unwrap();

    // 100 bytes is not 4 KiB aligned; expect validation to reject it with an Error
    let buf = vec![0xBB; 100];
    let res = file.aligned_write(0, &buf);
    assert!(
        res.is_err(),
        "Unaligned writes must be rejected by alignment validation"
    );
}

#[test]
fn test_simulated_file_not_found() {
    let backend = SimulatedBackend::new();
    let dir = tempdir().unwrap();
    let path = dir.path().join("nonexistent.seg");

    let res = backend.open_segment(&path, false);
    assert!(
        res.is_err(),
        "Opening a non-existent segment should return an error"
    );
}

#[test]
fn test_simulated_truncate_and_len() {
    let backend = SimulatedBackend::new();
    let dir = tempdir().unwrap();
    let path = dir.path().join("truncate.seg");

    backend.preallocate_and_prezero(&path, 16384).unwrap();
    let mut file = backend.open_segment(&path, false).unwrap();
    assert_eq!(file.len().unwrap(), 16384);

    file.truncate(4096).unwrap();
    assert_eq!(file.len().unwrap(), 4096);

    let mut buf = vec![0u8; 4096];
    let n = file.aligned_read(4096, &mut buf).unwrap();
    assert_eq!(n, 0);
}
