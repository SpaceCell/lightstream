//! # Asynchronous disk byte stream
//!
//! Wraps a file in a [`Stream`] that yields fixed-size byte chunks.
//!
//! ## Overview
//! - Uses Tokio [`File`] + [`BufReader`] under the hood.
//! - Supports async backpressure via `poll_next`.
//! - One copy into a `Vec64<u8>` output buffer per chunk.
//! - Chunk size controlled by [`BufferChunkSize`].
//!
//! ## Use cases
//! - Ingest large files without loading them fully into memory.
//! - Feed disk I/O directly into async pipelines.

use std::io;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use minarrow::{Vec64, vec64};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};

use crate::enums::BufferChunkSize;

/// A `Stream` that reads a file in fixed-size byte chunks.
///
/// ### Includes:
/// - Tokio + `BufReader` based
/// - Async back-pressure support via `poll_next`
/// - One copy to Vec<u8> output buffer
/// - control of chunk size via `BufferChunkSize`
/// ### Use cases:
/// - Ingest large files without loading the full content into memory
/// - Integrate disk I/O into async pipelines
pub struct DiskByteStream {
    /// Buffered reader over the file.
    reader: BufReader<File>,
    /// End-of-file flag, prevents further reads after completion.
    eof: bool,
    /// Reusable buffer to avoid reallocating per `poll_next`.
    buf: Vec64<u8>,
    /// Configured chunk size in bytes.
    chunk_size: usize,
}

impl DiskByteStream {
    /// Open a file as a `DiskByteStream`.
    ///
    /// ### Parameters:
    /// - `path`: Path to the file.
    /// - `size`: Chunk size strategy (`BufferChunkSize`).
    ///
    /// ### Returns:
    /// - `Ok(DiskByteStream)` if successful.
    /// - `Err(io::Error)` on file open failure.
    pub async fn open(path: impl AsRef<Path>, size: BufferChunkSize) -> io::Result<Self> {
        let chunk_size = size.chunk_size();
        let file = File::open(path).await?;
        Ok(Self {
            reader: BufReader::with_capacity(chunk_size, file),
            eof: false,
            buf: vec64![0u8; chunk_size],
            chunk_size,
        })
    }
}

impl Stream for DiskByteStream {
    /// Yield the next chunk of bytes from the file.
    ///
    /// - On success: returns `Ok(Vec<u8>)` containing up to `chunk_size` bytes.
    /// - On EOF: returns `None`.
    /// - On I/O error: returns `Err(io::Error)`.
    type Item = Result<Vec64<u8>, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();

        if me.eof {
            return Poll::Ready(None);
        }

        // read directly into the internal staging buffer
        let read = {
            // safety: we never move `reader`
            let mut fut = Box::pin(me.reader.read(&mut me.buf[..me.chunk_size]));
            futures_core::ready!(fut.as_mut().poll(cx))
        };

        match read {
            Ok(0) => {
                me.eof = true;
                Poll::Ready(None) // EOF
            }
            Ok(n) => {
                // move the filled buffer out
                let mut out = std::mem::replace(
                    &mut me.buf,
                    vec64![0u8; me.chunk_size], // new staging buf
                );
                out.truncate(n); // keep only the bytes we read
                Poll::Ready(Some(Ok(out))) // hand ownership to caller
            }
            Err(e) => {
                me.eof = true;
                Poll::Ready(Some(Err(e)))
            }
        }
    }
}

// Implement AsyncRead for DiskByteStream by forwarding to BufReader<File>
impl AsyncRead for DiskByteStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // Safety: it is safe to get mutable access because Unpin is implemented.
        let me = self.get_mut();
        Pin::new(&mut me.reader).poll_read(cx, buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use std::fs::File as StdFile;
    use std::io::Write;
    use std::path::PathBuf;
    use tokio::runtime::Runtime;

    fn create_test_file(size: usize, pattern: u8) -> PathBuf {
        let tmp_path = std::env::temp_dir().join(format!("disk_bytestream_test_{}.bin", pattern));
        let mut f = StdFile::create(&tmp_path).expect("create temp file");
        f.write_all(&vec![pattern; size]).expect("write data");
        tmp_path
    }

    #[test]
    fn test_disk_bytestream_fileio_chunks() {
        const FILE_SIZE: usize = 4 * 1024 * 1024; // 4 MiB

        let path = create_test_file(FILE_SIZE, 0xAA);

        let rt = Runtime::new().expect("create runtime");
        rt.block_on(async {
            let stream = DiskByteStream::open(&path, BufferChunkSize::FileIO)
                .await
                .expect("open stream");

            let mut s = Box::pin(stream);

            let mut count = 0usize;
            let mut total_bytes = 0usize;

            while let Some(item) = s.next().await {
                let chunk = item.expect("chunk read error");
                assert!(chunk.len() <= BufferChunkSize::FileIO.chunk_size());
                for b in chunk.iter() {
                    assert_eq!(*b, 0xAA);
                }
                count += 1;
                total_bytes += chunk.len();
            }

            assert!(count > 0);
            assert_eq!(total_bytes, FILE_SIZE);
        });

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_disk_bytestream_custom_chunk() {
        const FILE_SIZE: usize = 1 * 1024 * 1024; // 1 MiB
        const CHUNK: usize = 128 * 1024; // 128 KiB

        let path = create_test_file(FILE_SIZE, 0x55);

        let rt = Runtime::new().expect("create runtime");
        rt.block_on(async {
            let stream = DiskByteStream::open(&path, BufferChunkSize::Custom(CHUNK))
                .await
                .expect("open stream");

            let mut s = Box::pin(stream);

            let mut count = 0usize;
            let mut total_bytes = 0usize;

            while let Some(item) = s.next().await {
                let chunk = item.expect("chunk read error");
                assert!(chunk.len() <= CHUNK);
                for b in chunk.iter() {
                    assert_eq!(*b, 0x55);
                }
                count += 1;
                total_bytes += chunk.len();
            }

            assert!(count > 0);
            assert_eq!(total_bytes, FILE_SIZE);
        });

        std::fs::remove_file(path).unwrap();
    }
}
