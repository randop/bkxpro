//! Dedicated I/O thread that owns the segment [`SegmentIo`] backend.
//!
//! `io_uring` rings are thread-bound (SINGLE_ISSUER): the ring must be created and
//! used by one thread, so the uploader never touches [`SegmentIo`] from the tokio
//! workers. Instead every local operation - digest validation, aligned streaming
//! reads, size queries, unlink - is a command to this thread. Reads stream into
//! owned aligned chunks (see [`AlignedChunk`]) that the tokio side hands to the
//! blob store without copying.

use std::{
    error::Error as StdError,
    fmt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
};

use bkxpro_codec::BLOCK_SIZE;
use bkxpro_segmenter::{AlignedBuffer, SegmentIo};
use bytes::Bytes;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::{
    blob::StreamItem,
    budget::{BudgetGuard, ByteBudget},
    digest::{footer_bounds, DigestError, SegmentDigest},
    ioprio::{set_current_thread_priority, IoPriority},
};

/// Errors from the segment I/O worker.
#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("segment I/O failed: {message}")]
    Io {
        message: String,
        /// True when the backend reported ENOENT.
        not_found: bool,
    },
    #[error("segment {segment_id} failed codec validation: {detail}")]
    Codec { segment_id: u64, detail: String },
    #[error("segment {segment_id} header names segment {found}")]
    IdMismatch { segment_id: u64, found: u64 },
    #[error("aligned allocation failed: {0}")]
    Alloc(String),
    #[error("stream chunk size {chunk} is not a non-zero 4 KiB multiple")]
    InvalidChunk { chunk: usize },
    #[error("stream chunk size {chunk} exceeds the byte budget {budget}")]
    BudgetTooSmall { chunk: usize, budget: usize },
    #[error("read arguments are not 4 KiB aligned: offset {offset}, length {length}")]
    MisalignedRead { offset: u64, length: usize },
    #[error("read is out of bounds for {path}: offset {offset}, length {length}, file {file_len}")]
    OutOfBounds {
        path: String,
        offset: u64,
        length: usize,
        file_len: u64,
    },
    #[error("segment I/O worker is shut down")]
    Closed,
}

impl WorkerError {
    fn io<E: StdError + 'static>(context: impl fmt::Display, error: E) -> Self {
        let not_found = (&error as &dyn StdError)
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound);
        Self::Io {
            message: format!("{context}: {error}"),
            not_found,
        }
    }

    /// True when the underlying failure was "file does not exist".
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            WorkerError::Io {
                not_found: true,
                ..
            }
        )
    }
}

/// An owned, budgeted, 4 KiB-aligned read chunk handed to the blob store as
/// `Bytes`. Dropping the last `Bytes` clone frees the allocation and releases the
/// byte-budget permit, which is what makes reader memory bounded.
pub struct AlignedChunk {
    buffer: SendBuffer,
    /// Held until the chunk is released; never read.
    _permit: BudgetGuard,
}

/// Movable wrapper around [`AlignedBuffer`].
///
/// `AlignedBuffer` holds a `NonNull<u8>`, which is conservatively `!Send`, but it
/// owns its allocation exclusively for its whole lifetime and frees it in `Drop`
/// through the global allocator, so moving it across threads is sound. The wrapper
/// exists solely to let a chunk cross from the I/O thread to the tokio side.
struct SendBuffer(AlignedBuffer);

// SAFETY: the wrapped buffer uniquely owns a private heap allocation; no aliasing,
// borrowed data, or thread affinity is involved in moving or dropping it.
unsafe impl Send for SendBuffer {}

impl AsRef<[u8]> for AlignedChunk {
    fn as_ref(&self) -> &[u8] {
        &self.buffer.0
    }
}

enum WorkerCommand {
    Stat {
        path: PathBuf,
        expected_segment_id: u64,
        reply: oneshot::Sender<Result<SegmentDigest, WorkerError>>,
    },
    Stream {
        path: PathBuf,
        expected_segment_id: u64,
        chunk_bytes: usize,
        budget: Arc<ByteBudget>,
        sink: mpsc::Sender<StreamItem>,
        reply: oneshot::Sender<Result<(), WorkerError>>,
    },
    Read {
        path: PathBuf,
        offset: u64,
        length: usize,
        reply: oneshot::Sender<Result<Bytes, WorkerError>>,
    },
    FileLen {
        path: PathBuf,
        reply: oneshot::Sender<Result<u64, WorkerError>>,
    },
    Unlink {
        path: PathBuf,
        directory: PathBuf,
        reply: oneshot::Sender<Result<(), WorkerError>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

/// Handle to the I/O thread. Dropping the handle detaches the thread; calling
/// [`SegmentIoWorker::shutdown`] joins it.
pub struct SegmentIoWorker {
    tx: mpsc::UnboundedSender<WorkerCommand>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl SegmentIoWorker {
    /// Spawn the worker thread with the default uploader I/O priority
    /// ([`IoPriority::default`]: best-effort, lowest level).
    pub fn spawn<I, F>(factory: F) -> Result<Arc<Self>, WorkerError>
    where
        I: SegmentIo,
        F: FnOnce() -> Result<I, I::Error> + Send + 'static,
    {
        Self::spawn_with_io_priority(factory, IoPriority::default())
    }

    /// Spawn the worker thread, lowering its I/O priority before the backend is
    /// created. `factory` runs *on the worker thread* so the ring (or any other
    /// thread-bound backend) is created by its single issuer.
    ///
    /// A refused `ioprio_set` is logged and ignored: the capability is optional, and
    /// cgroup v2 `io.weight` is the alternative way to isolate upload traffic.
    pub fn spawn_with_io_priority<I, F>(
        factory: F,
        priority: IoPriority,
    ) -> Result<Arc<Self>, WorkerError>
    where
        I: SegmentIo,
        F: FnOnce() -> Result<I, I::Error> + Send + 'static,
    {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let join = thread::Builder::new()
            .name("bkxpro-segment-io".to_string())
            .spawn(move || {
                if let Err(error) = set_current_thread_priority(priority) {
                    tracing::warn!(
                        %error,
                        "could not lower the segment I/O thread priority; \
                         configure cgroup v2 io.weight for upload isolation"
                    );
                }
                match factory() {
                    Ok(mut io) => run_worker(&mut io, &mut rx),
                    Err(error) => {
                        let message = format!("segment I/O backend failed to start: {error}");
                        while let Some(command) = rx.blocking_recv() {
                            if let WorkerCommand::Shutdown { reply } = command {
                                let _ = reply.send(());
                                break;
                            }
                            fail_command(command, &message);
                        }
                    }
                }
            })
            .map_err(|error| WorkerError::io("spawning the segment I/O thread", error))?;
        Ok(Arc::new(Self {
            tx,
            join: Mutex::new(Some(join)),
        }))
    }

    /// Validate a sealed segment and return its digest.
    pub async fn stat(
        &self,
        path: impl Into<PathBuf>,
        expected_segment_id: u64,
    ) -> Result<SegmentDigest, WorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send(WorkerCommand::Stat {
            path: path.into(),
            expected_segment_id,
            reply: reply_tx,
        })?;
        reply_rx.await.map_err(|_| WorkerError::Closed)?
    }

    /// Stream a sealed segment into `sink` in `chunk_bytes` aligned chunks.
    pub async fn stream(
        &self,
        path: impl Into<PathBuf>,
        expected_segment_id: u64,
        chunk_bytes: usize,
        budget: Arc<ByteBudget>,
        sink: mpsc::Sender<StreamItem>,
    ) -> Result<(), WorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send(WorkerCommand::Stream {
            path: path.into(),
            expected_segment_id,
            chunk_bytes,
            budget,
            sink,
            reply: reply_tx,
        })?;
        reply_rx.await.map_err(|_| WorkerError::Closed)?
    }

    /// Read exactly `length` bytes at `offset`; both must be 4 KiB aligned.
    pub async fn read(
        &self,
        path: impl Into<PathBuf>,
        offset: u64,
        length: usize,
    ) -> Result<Bytes, WorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send(WorkerCommand::Read {
            path: path.into(),
            offset,
            length,
            reply: reply_tx,
        })?;
        reply_rx.await.map_err(|_| WorkerError::Closed)?
    }

    /// Current file length in bytes.
    pub async fn file_len(&self, path: impl Into<PathBuf>) -> Result<u64, WorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send(WorkerCommand::FileLen {
            path: path.into(),
            reply: reply_tx,
        })?;
        reply_rx.await.map_err(|_| WorkerError::Closed)?
    }

    /// Unlink a segment and make the directory-entry change durable. Idempotent:
    /// a missing file is a success.
    pub async fn unlink(
        &self,
        path: impl Into<PathBuf>,
        directory: impl Into<PathBuf>,
    ) -> Result<(), WorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send(WorkerCommand::Unlink {
            path: path.into(),
            directory: directory.into(),
            reply: reply_tx,
        })?;
        reply_rx.await.map_err(|_| WorkerError::Closed)?
    }

    /// Stop the worker thread and join it.
    pub async fn shutdown(&self) -> Result<(), WorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .tx
            .send(WorkerCommand::Shutdown { reply: reply_tx })
            .is_err()
        {
            self.join_thread();
            return Ok(());
        }
        let _ = reply_rx.await;
        self.join_thread();
        Ok(())
    }

    fn send(&self, command: WorkerCommand) -> Result<(), WorkerError> {
        self.tx.send(command).map_err(|_| WorkerError::Closed)
    }

    fn join_thread(&self) {
        let join = self.join.lock().unwrap().take();
        if let Some(join) = join {
            let _ = join.join();
        }
    }
}

impl fmt::Debug for SegmentIoWorker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SegmentIoWorker").finish_non_exhaustive()
    }
}

fn fail_command(command: WorkerCommand, message: &str) {
    let error = || WorkerError::Io {
        message: message.to_string(),
        not_found: false,
    };
    match command {
        WorkerCommand::Stat { reply, .. } => {
            let _ = reply.send(Err(error()));
        }
        WorkerCommand::Stream { reply, .. } => {
            let _ = reply.send(Err(error()));
        }
        WorkerCommand::Read { reply, .. } => {
            let _ = reply.send(Err(error()));
        }
        WorkerCommand::FileLen { reply, .. } => {
            let _ = reply.send(Err(error()));
        }
        WorkerCommand::Unlink { reply, .. } => {
            let _ = reply.send(Err(error()));
        }
        WorkerCommand::Shutdown { reply } => {
            let _ = reply.send(());
        }
    }
}

fn run_worker<I: SegmentIo>(io: &mut I, commands: &mut mpsc::UnboundedReceiver<WorkerCommand>) {
    while let Some(command) = commands.blocking_recv() {
        match command {
            WorkerCommand::Stat {
                path,
                expected_segment_id,
                reply,
            } => {
                let _ = reply.send(stat_segment(io, &path, expected_segment_id));
            }
            WorkerCommand::Stream {
                path,
                expected_segment_id,
                chunk_bytes,
                budget,
                sink,
                reply,
            } => {
                let result =
                    stream_segment(io, &path, expected_segment_id, chunk_bytes, &budget, &sink);
                if let Err(error) = &result {
                    // The blob store may already be waiting for a terminator.
                    let _ = sink.blocking_send(StreamItem::Failed(error.to_string()));
                }
                let _ = reply.send(result);
            }
            WorkerCommand::Read {
                path,
                offset,
                length,
                reply,
            } => {
                let _ = reply.send(read_exact(io, &path, offset, length));
            }
            WorkerCommand::FileLen { path, reply } => {
                let _ = reply.send(file_len(io, &path));
            }
            WorkerCommand::Unlink {
                path,
                directory,
                reply,
            } => {
                let _ = reply.send(unlink_segment(io, &path, &directory));
            }
            WorkerCommand::Shutdown { reply } => {
                let _ = reply.send(());
                break;
            }
        }
    }
}

fn open_for_read<I: SegmentIo>(io: &mut I, path: &Path) -> Result<I::File, WorkerError> {
    io.open_segment_for_read(path)
        .map_err(|error| WorkerError::io(format!("opening {}", path.display()), error))
}

fn file_len<I: SegmentIo>(io: &mut I, path: &Path) -> Result<u64, WorkerError> {
    let file = open_for_read(io, path)?;
    io.file_size(&file)
        .map_err(|error| WorkerError::io(format!("sizing {}", path.display()), error))
}

/// Validate one sealed segment and extract its digest.
pub fn stat_segment<I: SegmentIo>(
    io: &mut I,
    path: &Path,
    expected_segment_id: u64,
) -> Result<SegmentDigest, WorkerError> {
    let file = open_for_read(io, path)?;
    let file_len = io
        .file_size(&file)
        .map_err(|error| WorkerError::io(format!("sizing {}", path.display()), error))?;
    let codec = |detail: String| WorkerError::Codec {
        segment_id: expected_segment_id,
        detail,
    };
    let map_digest = |error: DigestError| match error {
        DigestError::TooSmall { len, minimum } => WorkerError::Codec {
            segment_id: expected_segment_id,
            detail: format!("segment is {len} bytes, below the {minimum}-byte minimum"),
        },
        other => codec(other.to_string()),
    };

    let mut header_block = AlignedBuffer::new_zeroed(BLOCK_SIZE)
        .map_err(|error| WorkerError::Alloc(error.to_string()))?;
    io.read_aligned(&file, 0, &mut header_block)
        .map_err(|error| WorkerError::io("reading the segment header", error))?;

    let tail_offset = file_len.saturating_sub(BLOCK_SIZE as u64);
    let mut tail_block = AlignedBuffer::new_zeroed(BLOCK_SIZE)
        .map_err(|error| WorkerError::Alloc(error.to_string()))?;
    if tail_offset > 0 {
        io.read_aligned(&file, tail_offset, &mut tail_block)
            .map_err(|error| WorkerError::io("reading the segment trailer block", error))?;
    }

    // The footer bounds come from the trailer; recover them before sizing the
    // footer read. A digest error here is final: these bytes are not a segment.
    let (footer_start, footer_len) = footer_bounds(&tail_block, file_len).map_err(map_digest)?;
    let footer_end = footer_start + footer_len as u64;
    let aligned_end = footer_end.div_ceil(BLOCK_SIZE as u64) * BLOCK_SIZE as u64;
    let read_len = (aligned_end - footer_start) as usize;
    let mut footer_block = AlignedBuffer::new_zeroed(read_len)
        .map_err(|error| WorkerError::Alloc(error.to_string()))?;
    io.read_aligned(&file, footer_start, &mut footer_block)
        .map_err(|error| WorkerError::io("reading the segment footer", error))?;
    let footer_bytes = Bytes::copy_from_slice(&footer_block[..footer_len as usize]);

    let digest = SegmentDigest::parse(file_len, &header_block, &tail_block, footer_bytes)
        .map_err(map_digest)?;
    if digest.segment_id != expected_segment_id {
        return Err(WorkerError::IdMismatch {
            segment_id: expected_segment_id,
            found: digest.segment_id,
        });
    }
    Ok(digest)
}

/// Stream a validated sealed segment into `sink` in aligned chunks.
pub fn stream_segment<I: SegmentIo>(
    io: &mut I,
    path: &Path,
    expected_segment_id: u64,
    chunk_bytes: usize,
    budget: &Arc<ByteBudget>,
    sink: &mpsc::Sender<StreamItem>,
) -> Result<(), WorkerError> {
    let digest = stat_segment(io, path, expected_segment_id)?;
    if chunk_bytes == 0 || chunk_bytes % BLOCK_SIZE != 0 {
        return Err(WorkerError::InvalidChunk { chunk: chunk_bytes });
    }
    let file = open_for_read(io, path)?;

    let mut offset = 0u64;
    let mut crc32c = 0u32;
    while offset < digest.file_len {
        let remaining = (digest.file_len - offset) as usize;
        let length = chunk_bytes.min(remaining);
        let Some(permit) = budget.acquire(length) else {
            return Err(WorkerError::BudgetTooSmall {
                chunk: length,
                budget: budget.capacity(),
            });
        };
        let mut buffer = AlignedBuffer::new_zeroed(length)
            .map_err(|error| WorkerError::Alloc(error.to_string()))?;
        io.read_aligned(&file, offset, &mut buffer)
            .map_err(|error| {
                WorkerError::io(format!("reading {} at {offset}", path.display()), error)
            })?;
        crc32c = crc32c::crc32c_append(crc32c, &buffer);
        let chunk = Bytes::from_owner(AlignedChunk {
            buffer: SendBuffer(buffer),
            _permit: permit,
        });
        if sink.blocking_send(StreamItem::Chunk(chunk)).is_err() {
            return Err(WorkerError::Closed);
        }
        offset += length as u64;
    }

    let _ = sink.blocking_send(StreamItem::End {
        len: digest.file_len,
        crc32c,
    });
    Ok(())
}

fn read_exact<I: SegmentIo>(
    io: &mut I,
    path: &Path,
    offset: u64,
    length: usize,
) -> Result<Bytes, WorkerError> {
    if offset % BLOCK_SIZE as u64 != 0 || length == 0 || length % BLOCK_SIZE != 0 {
        return Err(WorkerError::MisalignedRead { offset, length });
    }
    let file = open_for_read(io, path)?;
    let file_len = io
        .file_size(&file)
        .map_err(|error| WorkerError::io(format!("sizing {}", path.display()), error))?;
    if offset + length as u64 > file_len {
        return Err(WorkerError::OutOfBounds {
            path: path.display().to_string(),
            offset,
            length,
            file_len,
        });
    }
    let mut buffer =
        AlignedBuffer::new_zeroed(length).map_err(|error| WorkerError::Alloc(error.to_string()))?;
    io.read_aligned(&file, offset, &mut buffer)
        .map_err(|error| {
            WorkerError::io(format!("reading {} at {offset}", path.display()), error)
        })?;
    Ok(Bytes::copy_from_slice(&buffer))
}

fn unlink_segment<I: SegmentIo>(
    io: &mut I,
    path: &Path,
    directory: &Path,
) -> Result<(), WorkerError> {
    // No pre-flight `exists()` check: the backend's own namespace is authoritative
    // (a simulated or registered-file backend is not the visible filesystem), and a
    // missing file is reported as ENOENT which is success for an idempotent unlink.
    match io.unlink(path, directory) {
        Ok(()) => Ok(()),
        Err(error) => {
            let mapped = WorkerError::io(format!("unlinking {}", path.display()), error);
            if mapped.is_not_found() {
                Ok(())
            } else {
                Err(mapped)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bkxpro_io::simulated::SimulatedSegmentIo;

    use super::*;
    use crate::test_util::{install, sealed_image};

    /// Spawn a worker that owns `io`; the backend then lives exclusively on the
    /// worker thread, which is what makes it usable with thread-bound rings.
    fn worker_with_io(io: SimulatedSegmentIo) -> Arc<SegmentIoWorker> {
        SegmentIoWorker::spawn(move || Ok(io)).unwrap()
    }

    #[tokio::test]
    async fn worker_thread_uses_the_requested_io_priority_when_permitted() {
        use crate::ioprio::{current_thread_priority, set_current_thread_priority, IoPriority};

        // Probe the capability first: containers commonly refuse ioprio_set, and the
        // worker only logs a warning in that case.
        let before = match current_thread_priority() {
            Ok(priority) => priority,
            Err(error) => {
                eprintln!("skipping: ioprio_get is not permitted here: {error}");
                return;
            }
        };
        if let Err(error) = set_current_thread_priority(IoPriority::BestEffort(5)) {
            eprintln!("skipping: ioprio_set is not permitted here: {error}");
            return;
        }
        let _ = set_current_thread_priority(before);

        let (reported_tx, reported_rx) = std::sync::mpsc::channel();
        let worker = SegmentIoWorker::spawn_with_io_priority(
            move || {
                reported_tx.send(current_thread_priority()).unwrap();
                Ok(SimulatedSegmentIo::new())
            },
            IoPriority::BestEffort(5),
        )
        .unwrap();

        assert_eq!(
            reported_rx.recv().unwrap().unwrap(),
            IoPriority::BestEffort(5)
        );
        worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stat_validates_and_extracts_digest() {
        let mut io = SimulatedSegmentIo::new();
        let path = PathBuf::from("/sim/7.seg");
        let image = sealed_image(7, 42, 3);
        install(&mut io, &path, &image);
        let worker = worker_with_io(io);

        let stat = worker.stat(&path, 7).await.unwrap();
        assert_eq!(stat.segment_id, 7);
        assert_eq!(stat.writer_epoch, 42);
        assert_eq!(stat.file_len, image.len() as u64);
        assert_eq!(stat.entry_count, 3);
        assert_eq!((stat.min_ledger, stat.max_ledger), (1, 1));
        assert!(stat.footer_start >= bkxpro_codec::HEADER_SIZE as u64);
        assert!(stat.footer_start < stat.file_len);

        worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stat_rejects_id_mismatch_and_missing_files() {
        let mut io = SimulatedSegmentIo::new();
        let path = PathBuf::from("/sim/8.seg");
        install(&mut io, &path, &sealed_image(8, 1, 2));
        let worker = worker_with_io(io);

        let error = worker.stat(&path, 9).await.unwrap_err();
        assert!(matches!(error, WorkerError::IdMismatch { .. }));

        let error = worker.stat("/sim/absent.seg", 9).await.unwrap_err();
        assert!(error.is_not_found());

        worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stream_emits_every_byte_with_checksum() {
        let mut io = SimulatedSegmentIo::new();
        let path = PathBuf::from("/sim/9.seg");
        let image = sealed_image(9, 3, 8);
        install(&mut io, &path, &image);
        let worker = worker_with_io(io);

        let budget = ByteBudget::new(8192);
        let (sink, mut stream) = mpsc::channel(4);
        let stream_task = {
            let worker = Arc::clone(&worker);
            let path = path.clone();
            let budget = Arc::clone(&budget);
            tokio::spawn(async move { worker.stream(&path, 9, 4096, budget, sink).await })
        };

        let mut collected = Vec::new();
        let mut reported = None;
        while let Some(item) = stream.recv().await {
            match item {
                StreamItem::Chunk(chunk) => {
                    assert!(budget.in_use() > 0, "chunk should hold a permit");
                    collected.extend_from_slice(&chunk);
                }
                StreamItem::End { len, crc32c } => reported = Some((len, crc32c)),
                StreamItem::Failed(message) => panic!("stream failed: {message}"),
            }
        }
        stream_task.await.unwrap().unwrap();

        assert_eq!(collected, image.as_ref());
        let (len, crc) = reported.unwrap();
        assert_eq!(len, image.len() as u64);
        assert_eq!(crc, crc32c::crc32c(&image));
        assert_eq!(budget.in_use(), 0, "all permits released");
        assert!(budget.high_water() <= budget.capacity());

        worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stream_backpressure_bounds_inflight_bytes() {
        let mut io = SimulatedSegmentIo::new();
        let path = PathBuf::from("/sim/10.seg");
        let image = sealed_image(10, 3, 16);
        install(&mut io, &path, &image);
        let worker = worker_with_io(io);

        let chunk = 4096;
        let budget = ByteBudget::new(chunk * 2);
        let (sink, mut stream) = mpsc::channel(2);
        let stream_task = {
            let worker = Arc::clone(&worker);
            let path = path.clone();
            let budget = Arc::clone(&budget);
            tokio::spawn(async move { worker.stream(&path, 10, chunk, budget, sink).await })
        };

        let mut peak = 0usize;
        let mut seen = 0usize;
        while let Some(item) = stream.recv().await {
            match item {
                StreamItem::Chunk(chunk) => {
                    peak = peak.max(budget.in_use());
                    seen += chunk.len();
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
                StreamItem::End { .. } => break,
                StreamItem::Failed(message) => panic!("stream failed: {message}"),
            }
        }
        stream_task.await.unwrap().unwrap();
        assert_eq!(seen, image.len());
        assert!(peak <= 2 * 4096, "budget bounded in-flight bytes: {peak}");

        worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stream_fails_when_the_consumer_disappears() {
        let mut io = SimulatedSegmentIo::new();
        let path = PathBuf::from("/sim/11.seg");
        install(&mut io, &path, &sealed_image(11, 1, 2));
        let worker = worker_with_io(io);

        let budget = ByteBudget::new(4096);
        let (sink, stream) = mpsc::channel(1);
        drop(stream);
        let error = worker
            .stream(&path, 11, 4096, budget, sink)
            .await
            .unwrap_err();
        assert!(matches!(error, WorkerError::Closed));

        worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn read_and_unlink_are_idempotent() {
        let mut io = SimulatedSegmentIo::new();
        let path = PathBuf::from("/sim/12.seg");
        let image = sealed_image(12, 1, 2);
        install(&mut io, &path, &image);
        let worker = worker_with_io(io);

        let block = worker.read(&path, 0, 4096).await.unwrap();
        assert_eq!(block.as_ref(), &image[..4096]);
        assert_eq!(worker.file_len(&path).await.unwrap(), image.len() as u64);

        let error = worker.read(&path, 0, 4096 * 1000).await.unwrap_err();
        assert!(matches!(error, WorkerError::OutOfBounds { .. }));

        worker
            .unlink(&path, Path::new("/sim"))
            .await
            .expect("unlink");
        worker
            .unlink(&path, Path::new("/sim"))
            .await
            .expect("unlink is idempotent");
        assert!(worker.file_len(&path).await.unwrap_err().is_not_found());

        worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_is_clean_and_further_commands_fail() {
        let worker = worker_with_io(SimulatedSegmentIo::new());
        worker.shutdown().await.unwrap();
        let error = worker.stat("/sim/13.seg", 13).await.unwrap_err();
        assert!(matches!(error, WorkerError::Closed));
    }
}
