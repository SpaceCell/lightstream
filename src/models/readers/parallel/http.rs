//! # Parallel HTTP/2 table reader
//!
//! Accepts several concurrent HTTP/2 request streams on one server
//! connection and merges them into one table stream. Tables arrive from
//! every stream as they decode; ordering is preserved within a stream,
//! not across the set - the mirror of
//! [`HttpParallelTableWriter`](crate::models::writers::parallel::http::HttpParallelTableWriter).
//!
//! The h2 server connection is the I/O driver for the in-flight request
//! bodies, so a background task keeps it polled while the accepted
//! streams decode. Unlike QUIC, where quinn drives connections in the
//! background, an h2 server makes progress only while its connection is
//! polled.
//!
//! Implements `Stream<Item = io::Result<Table>>`, so it drives with
//! `StreamExt` like any other reader.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;
use futures_util::stream::{SelectAll, select_all};
use http::Response;
use minarrow::Table;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

use crate::models::readers::http::HttpTableReader;
use crate::traits::parallel_transport_reader::ParallelTransportReader;

/// Per-stream flow-control window the server advertises on the handshake.
/// HTTP/2 upload throughput is bounded by the receiver's window, and h2's
/// 64 KiB default forces a WINDOW_UPDATE round-trip every 64 KiB. At 8 MiB
/// a multi-MiB Arrow batch ships without that stall, which on a
/// few-millisecond cross-host link would otherwise dominate the transfer.
const STREAM_WINDOW_BYTES: u32 = 8 * 1024 * 1024;

/// Async Arrow IPC reader that merges several concurrent HTTP/2 request
/// streams on one server connection into a single table stream.
pub struct HttpParallelTableReader {
    inner: SelectAll<HttpTableReader>,
    driver: JoinHandle<()>,
    stream_count: usize,
}

impl HttpParallelTableReader {
    /// Accept `stream_count` request streams on an established h2 server
    /// `connection` and merge them. Each accepted request body is decoded
    /// as an independent Arrow IPC stream.
    ///
    /// A headers-only 200 is returned on each stream so the client's
    /// response drain resolves. The request body keeps uploading on the
    /// other half of the stream.
    pub async fn accept<T>(
        mut connection: h2::server::Connection<T, Bytes>,
        stream_count: usize,
    ) -> io::Result<Self>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        assert!(stream_count >= 1, "stream_count must be at least 1");
        let mut readers = Vec::with_capacity(stream_count);
        for _ in 0..stream_count {
            let (request, mut respond) = connection
                .accept()
                .await
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "h2 connection closed before all streams were accepted",
                    )
                })?
                .map_err(io::Error::other)?;
            let response = Response::builder()
                .status(200)
                .body(())
                .map_err(io::Error::other)?;
            respond
                .send_response(response, true)
                .map_err(io::Error::other)?;
            readers.push(HttpTableReader::from_recv(request.into_body()));
        }
        // Keep polling the connection so the accepted request bodies
        // receive their data frames. The loop ends when the peer closes.
        // The Drop impl aborts it otherwise.
        let driver = tokio::spawn(async move { while connection.accept().await.is_some() {} });
        Ok(Self { inner: select_all(readers), driver, stream_count })
    }

    /// Run the h2 server handshake on an accepted TCP stream with
    /// upload-sized flow-control windows, then accept `stream_count`
    /// request streams.
    ///
    /// POST throughput is governed by the server's flow-control window, so
    /// this advertises [`STREAM_WINDOW_BYTES`] per stream and scales the
    /// connection window with `stream_count`.
    pub async fn from_tcp(tcp: TcpStream, stream_count: usize) -> io::Result<Self> {
        // Size the connection window to the sum of the per-stream windows
        // so every concurrent stream gets its full allowance and the
        // connection window itself never throttles aggregate upload,
        // saturating at u32::MAX.
        let connection_window =
            (stream_count as u64 * STREAM_WINDOW_BYTES as u64).min(u32::MAX as u64) as u32;
        let connection = h2::server::Builder::new()
            .initial_window_size(STREAM_WINDOW_BYTES)
            .initial_connection_window_size(connection_window)
            .handshake::<_, Bytes>(tcp)
            .await
            .map_err(io::Error::other)?;
        Self::accept(connection, stream_count).await
    }
}

impl Stream for HttpParallelTableReader {
    type Item = io::Result<Table>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        Pin::new(&mut me.inner).poll_next(cx)
    }
}

impl ParallelTransportReader for HttpParallelTableReader {
    fn stream_count(&self) -> usize {
        self.stream_count
    }

    async fn read_all_tables(mut self) -> io::Result<Vec<Table>> {
        let mut out = Vec::new();
        while let Some(item) = self.inner.next().await {
            out.push(item?);
        }
        Ok(out)
    }
}

impl Drop for HttpParallelTableReader {
    fn drop(&mut self) {
        self.driver.abort();
    }
}
