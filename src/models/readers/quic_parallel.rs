//! # Parallel QUIC table reader
//!
//! Accepts several concurrent QUIC streams on a single
//! [`quinn::Connection`] and merges them into one table stream. Tables
//! arrive from every stream as they decode; ordering is preserved
//! within a stream, not across the set - the mirror of
//! [`QuicParallelTableWriter`](crate::models::writers::quic_parallel::QuicParallelTableWriter).
//!
//! Implements `Stream<Item = io::Result<Table>>`, so it drives with
//! `StreamExt` like any other reader.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use futures_util::StreamExt;
use futures_util::stream::{SelectAll, select_all};
use minarrow::Table;
use quinn::Connection;

use crate::models::readers::quic::QuicTableReader;
use crate::traits::parallel_transport_reader::ParallelTransportReader;

/// Async Arrow IPC reader that merges several concurrent QUIC streams
/// on one connection into a single table stream.
pub struct QuicParallelTableReader {
    inner: SelectAll<QuicTableReader>,
    stream_count: usize,
}

impl QuicParallelTableReader {
    /// Accept `stream_count` unidirectional QUIC streams on `conn` and
    /// merge them. Each accepted stream is decoded as an independent
    /// Arrow IPC stream.
    pub async fn accept(conn: &Connection, stream_count: usize) -> io::Result<Self> {
        assert!(stream_count >= 1, "stream_count must be at least 1");
        let mut streams: Vec<QuicTableReader> = Vec::with_capacity(stream_count);
        for _ in 0..stream_count {
            let recv = conn.accept_uni().await.map_err(io::Error::other)?;
            streams.push(QuicTableReader::from_recv(recv));
        }
        Ok(Self { inner: select_all(streams), stream_count })
    }
}

impl Stream for QuicParallelTableReader {
    type Item = io::Result<Table>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        Pin::new(&mut me.inner).poll_next(cx)
    }
}

impl ParallelTransportReader for QuicParallelTableReader {
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
