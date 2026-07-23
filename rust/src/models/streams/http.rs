// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # HTTP/2 byte stream adapters
//!
//! Receive side: [`H2RecvRead`](crate::models::streams::http::H2RecvRead) wraps [`h2::RecvStream`] as `AsyncRead`
//! so it flows through [`AsyncReadByteStream`](crate::models::streams::async_read::AsyncReadByteStream) like every other transport.
//!
//! Send side: [`H2SendWrite`](crate::models::streams::http::H2SendWrite) wraps [`h2::SendStream<Bytes>`] as
//! `AsyncWrite` so `HttpTableWriter` can hold it inside `TableSink64`
//! exactly the same way `QuicTableWriter` holds a `quinn::SendStream`.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::models::streams::async_read::AsyncReadByteStream;

/// AsyncRead over an h2 response body.
///
/// Each `poll_read` pulls the next data frame (a refcounted `Bytes`
/// view into h2's internal frame buffer), copies up to the caller's
/// remaining capacity into their `Vec64` spare, and carries leftover
/// bytes across calls. Releases h2 flow-control capacity as bytes are
/// consumed so the peer keeps sending.
pub struct H2RecvRead {
    recv: h2::RecvStream,
    leftover: Bytes,
}

impl H2RecvRead {
    pub fn new(recv: h2::RecvStream) -> Self {
        Self {
            recv,
            leftover: Bytes::new(),
        }
    }
}

impl AsyncRead for H2RecvRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if !self.leftover.is_empty() {
                let n = self.leftover.len().min(buf.remaining());
                buf.put_slice(&self.leftover[..n]);
                self.leftover.advance(n);
                return Poll::Ready(Ok(()));
            }
            match Pin::new(&mut self.recv).poll_data(cx) {
                Poll::Ready(Some(Ok(data))) => {
                    let _ = self.recv.flow_control().release_capacity(data.len());
                    self.leftover = data;
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(io::Error::other(e))),
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// AsyncWrite over an h2 request body.
///
/// Wraps `h2::SendStream<Bytes>` to fit the `TableSink64<W: AsyncWrite>`
/// shape that every other lightstream transport writer uses. Each
/// `poll_write` reserves h2 flow-control capacity for the caller's
/// slice, awaits the grant, and ships a chunk via `send_data` -
/// equivalent to what quinn's own AsyncWrite impl does for QUIC.
///
/// `poll_shutdown` emits an empty `send_data` with `end_of_stream = true`
/// so the server sees a clean stream close after `TableSink64::poll_close`
/// has flushed the IPC EOS marker.
pub struct H2SendWrite {
    send: h2::SendStream<Bytes>,
    finished: bool,
}

impl H2SendWrite {
    pub fn new(send: h2::SendStream<Bytes>) -> Self {
        Self {
            send,
            finished: false,
        }
    }
}

impl AsyncWrite for H2SendWrite {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let me = self.get_mut();
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        // Request flow-control window equal to the caller's slice; h2
        // grants whatever the peer's WINDOW_UPDATE has unlocked, which
        // may be less than buf.len(). Partial writes are fine - the
        // caller (TableSink64::poll_flush) will loop.
        me.send.reserve_capacity(buf.len());
        let granted = match me.send.poll_capacity(cx) {
            Poll::Ready(Some(Ok(n))) => n,
            Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(io::Error::other(e))),
            Poll::Ready(None) => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "h2 send stream closed before capacity available",
                )));
            }
            Poll::Pending => return Poll::Pending,
        };
        if granted == 0 {
            return Poll::Pending;
        }

        let to_send = granted.min(buf.len());
        let chunk = Bytes::copy_from_slice(&buf[..to_send]);
        me.send.send_data(chunk, false).map_err(io::Error::other)?;
        Poll::Ready(Ok(to_send))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // h2 buffers internally and ships frames as flow-control permits;
        // there is no explicit user-level flush.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        if !me.finished {
            me.finished = true;
            me.send
                .send_data(Bytes::new(), true)
                .map_err(io::Error::other)?;
        }
        Poll::Ready(Ok(()))
    }
}

/// HTTP/2 byte stream. Same shape as
/// [`QuicByteStream`](crate::models::streams::quic::QuicByteStream) /
/// [`WebTransportByteStream`](crate::models::streams::webtransport::WebTransportByteStream).
pub type HttpByteStream = AsyncReadByteStream<H2RecvRead>;
