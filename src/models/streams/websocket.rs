// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # WebSocket byte stream adapters
//!
//! Provides [`WsRead`](crate::models::streams::websocket::WsRead) and [`WsWrite`](crate::models::streams::websocket::WsWrite) for WebSocket I/O over a raw TCP
//! stream extracted after the tungstenite handshake.
//!
//! WS frame parsing and construction happens inline with zero intermediate
//! allocations. Payload bytes flow between the TCP socket and the caller's
//! buffer via the standard [`AsyncRead`](tokio::io::AsyncRead) / [`AsyncWrite`](tokio::io::AsyncWrite) traits.
//!
//! [`WsRead`](crate::models::streams::websocket::WsRead) also implements [`Stream`](futures_core::Stream) yielding arena-backed [`SharedBuffer`](minarrow::structs::shared_buffer::SharedBuffer)
//! windows for consumers that prefer the StreamExt API.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures_core::Stream;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use minarrow::structs::shared_buffer::SharedBuffer;

use crate::models::frames::websocket::{
    self, MAX_HEADER_LEN, OPCODE_BINARY, OPCODE_CLOSE, OPCODE_PING,
};
use crate::models::streams::stream_arena::StreamArena;

// ---------------------------------------------------------------------------
// Read path
// ---------------------------------------------------------------------------

/// Read state machine for WebSocket frame parsing.
enum WsReadState {
    /// Accumulating the WS frame header (2-14 bytes).
    Header { filled: usize },
    /// Reading payload bytes into the caller's buffer.
    Payload {
        remaining: usize,
        masked: bool,
        mask_key: [u8; 4],
        mask_offset: usize,
    },
    /// Reading a ping payload before sending the pong.
    ReadingPing { remaining: usize, payload: Vec<u8> },
    /// Sending a pong response. Transitions to Header when done.
    SendingPong { buf: Vec<u8>, written: usize },
    /// Connection closed.
    Closed,
}

/// WebSocket read adapter over a raw TCP stream.
///
/// Parses WS frame headers in a fixed stack buffer and exposes unmasked
/// binary payload bytes via [`AsyncRead`]. Ping and close frames are
/// handled transparently - pongs are sent immediately via the shared
/// write half, matching standard WebSocket library behaviour.
///
/// Also implements [`Stream`] yielding arena-backed [`SharedBuffer`]
/// windows for zero-allocation streaming.
pub struct WsRead<R, W> {
    inner: R,
    writer: Arc<Mutex<W>>,
    state: WsReadState,
    header_buf: [u8; MAX_HEADER_LEN],
    arena: StreamArena,
    chunk_size: usize,
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> WsRead<R, W> {
    /// Wrap a raw TCP read half as a WebSocket reader.
    ///
    /// The `writer` is the shared write half used for sending pong
    /// responses to pings. The WebSocket handshake must already be complete.
    pub fn new(inner: R, writer: Arc<Mutex<W>>) -> Self {
        Self {
            inner,
            writer,
            state: WsReadState::Header { filled: 0 },
            header_buf: [0u8; MAX_HEADER_LEN],
            arena: StreamArena::new(),
            chunk_size: 64 * 1024,
        }
    }
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> AsyncRead for WsRead<R, W> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();

        loop {
            match &mut me.state {
                WsReadState::Closed => {
                    return Poll::Ready(Ok(()));
                }

                WsReadState::Header { filled } => {
                    let mut need = if *filled < 2 {
                        2
                    } else {
                        let masked = me.header_buf[1] & 0x80 != 0;
                        let len7 = me.header_buf[1] & 0x7F;
                        let extra = match len7 {
                            0..=125 => 0,
                            126 => 2,
                            _ => 8,
                        } + if masked { 4 } else { 0 };
                        2 + extra
                    };

                    while *filled < need {
                        let mut hdr_read = ReadBuf::new(&mut me.header_buf[*filled..need]);
                        match Pin::new(&mut me.inner).poll_read(cx, &mut hdr_read) {
                            Poll::Ready(Ok(())) => {
                                let n = hdr_read.filled().len();
                                if n == 0 {
                                    me.state = WsReadState::Closed;
                                    return Poll::Ready(Ok(()));
                                }
                                *filled += n;
                            }
                            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                            Poll::Pending => return Poll::Pending,
                        }
                        if *filled >= 2 {
                            let masked = me.header_buf[1] & 0x80 != 0;
                            let len7 = me.header_buf[1] & 0x7F;
                            let extra = match len7 {
                                0..=125 => 0usize,
                                126 => 2,
                                _ => 8,
                            } + if masked { 4 } else { 0 };
                            need = 2 + extra;
                        }
                    }

                    let ws = match websocket::parse_header(&me.header_buf[..*filled]) {
                        Some(h) => h,
                        None => {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "malformed WebSocket frame header",
                            )));
                        }
                    };

                    let payload_len = ws.payload_len as usize;

                    match ws.opcode {
                        OPCODE_BINARY | 0x0 => {
                            if payload_len == 0 {
                                me.state = WsReadState::Header { filled: 0 };
                                continue;
                            }
                            me.state = WsReadState::Payload {
                                remaining: payload_len,
                                masked: ws.masked,
                                mask_key: ws.mask_key,
                                mask_offset: 0,
                            };
                            continue;
                        }
                        OPCODE_CLOSE => {
                            me.state = WsReadState::Closed;
                            return Poll::Ready(Ok(()));
                        }
                        OPCODE_PING => {
                            if payload_len > 0 {
                                me.state = WsReadState::ReadingPing {
                                    remaining: payload_len,
                                    payload: Vec::with_capacity(payload_len),
                                };
                            } else {
                                // Empty ping - send empty pong immediately
                                let mut pong = vec![0u8; 2];
                                let n = websocket::write_pong_frame(&mut pong, &[]);
                                pong.truncate(n);
                                me.state = WsReadState::SendingPong {
                                    buf: pong,
                                    written: 0,
                                };
                            }
                            continue;
                        }
                        _ => {
                            me.state = WsReadState::Header { filled: 0 };
                            continue;
                        }
                    }
                }

                WsReadState::Payload {
                    remaining,
                    masked,
                    mask_key,
                    mask_offset,
                } => {
                    if *remaining == 0 {
                        me.state = WsReadState::Header { filled: 0 };
                        continue;
                    }

                    let max_read = (*remaining).min(buf.remaining());
                    if max_read == 0 {
                        return Poll::Ready(Ok(()));
                    }

                    let _before = buf.filled().len();
                    // Create a sub-ReadBuf limited to max_read bytes
                    let unfilled = buf.initialize_unfilled_to(max_read);
                    let mut sub = ReadBuf::new(unfilled);
                    match Pin::new(&mut me.inner).poll_read(cx, &mut sub) {
                        Poll::Ready(Ok(())) => {
                            let n = sub.filled().len();
                            buf.advance(n);
                            if n == 0 {
                                me.state = WsReadState::Closed;
                                return Poll::Ready(Ok(()));
                            }

                            if *masked {
                                let written = buf.filled_mut();
                                let start = written.len() - n;
                                for i in 0..n {
                                    written[start + i] ^= mask_key[(*mask_offset + i) % 4];
                                }
                                *mask_offset = (*mask_offset + n) % 4;
                            }

                            *remaining -= n;
                            if *remaining == 0 {
                                me.state = WsReadState::Header { filled: 0 };
                            }
                            return Poll::Ready(Ok(()));
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Pending => return Poll::Pending,
                    }
                }

                WsReadState::ReadingPing { remaining, payload } => {
                    let mut ping_buf = [0u8; 125];
                    let to_read = (*remaining).min(ping_buf.len());
                    let mut rb = ReadBuf::new(&mut ping_buf[..to_read]);
                    match Pin::new(&mut me.inner).poll_read(cx, &mut rb) {
                        Poll::Ready(Ok(())) => {
                            let n = rb.filled().len();
                            if n == 0 {
                                me.state = WsReadState::Closed;
                                return Poll::Ready(Ok(()));
                            }
                            payload.extend_from_slice(&ping_buf[..n]);
                            *remaining -= n;
                            if *remaining == 0 {
                                // Build pong frame and transition to sending it
                                let mut pong = vec![0u8; 2 + payload.len()];
                                let pong_len = websocket::write_pong_frame(&mut pong, payload);
                                pong.truncate(pong_len);
                                me.state = WsReadState::SendingPong {
                                    buf: pong,
                                    written: 0,
                                };
                            }
                            continue;
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Pending => return Poll::Pending,
                    }
                }

                WsReadState::SendingPong { buf, written } => {
                    let mut guard = me.writer.lock().unwrap();
                    let remaining = &buf[*written..];
                    match Pin::new(&mut *guard).poll_write(cx, remaining) {
                        Poll::Ready(Ok(n)) => {
                            *written += n;
                            if *written >= buf.len() {
                                me.state = WsReadState::Header { filled: 0 };
                            }
                            continue;
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> Stream for WsRead<R, W> {
    type Item = Result<SharedBuffer, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();

        loop {
            if me.arena.remaining() < me.chunk_size {
                me.arena.recycle_or_reset();
            }

            match &mut me.state {
                WsReadState::Closed => return Poll::Ready(None),

                WsReadState::Header { filled } => {
                    let mut need = if *filled < 2 {
                        2
                    } else {
                        let masked = me.header_buf[1] & 0x80 != 0;
                        let len7 = me.header_buf[1] & 0x7F;
                        let extra = match len7 {
                            0..=125 => 0,
                            126 => 2,
                            _ => 8,
                        } + if masked { 4 } else { 0 };
                        2 + extra
                    };

                    while *filled < need {
                        let mut hdr_read = ReadBuf::new(&mut me.header_buf[*filled..need]);
                        match Pin::new(&mut me.inner).poll_read(cx, &mut hdr_read) {
                            Poll::Ready(Ok(())) => {
                                let n = hdr_read.filled().len();
                                if n == 0 {
                                    me.state = WsReadState::Closed;
                                    return Poll::Ready(None);
                                }
                                *filled += n;
                            }
                            Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                            Poll::Pending => return Poll::Pending,
                        }
                        if *filled >= 2 {
                            let masked = me.header_buf[1] & 0x80 != 0;
                            let len7 = me.header_buf[1] & 0x7F;
                            let extra = match len7 {
                                0..=125 => 0usize,
                                126 => 2,
                                _ => 8,
                            } + if masked { 4 } else { 0 };
                            need = 2 + extra;
                        }
                    }

                    let ws = match websocket::parse_header(&me.header_buf[..*filled]) {
                        Some(h) => h,
                        None => {
                            return Poll::Ready(Some(Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "malformed WebSocket frame header",
                            ))));
                        }
                    };

                    let payload_len = ws.payload_len as usize;
                    match ws.opcode {
                        OPCODE_BINARY | 0x0 => {
                            if payload_len == 0 {
                                me.state = WsReadState::Header { filled: 0 };
                                continue;
                            }
                            me.state = WsReadState::Payload {
                                remaining: payload_len,
                                masked: ws.masked,
                                mask_key: ws.mask_key,
                                mask_offset: 0,
                            };
                            continue;
                        }
                        OPCODE_CLOSE => {
                            me.state = WsReadState::Closed;
                            return Poll::Ready(None);
                        }
                        OPCODE_PING => {
                            if payload_len > 0 {
                                me.state = WsReadState::ReadingPing {
                                    remaining: payload_len,
                                    payload: Vec::with_capacity(payload_len),
                                };
                            } else {
                                let mut pong = vec![0u8; 2];
                                let n = websocket::write_pong_frame(&mut pong, &[]);
                                pong.truncate(n);
                                me.state = WsReadState::SendingPong {
                                    buf: pong,
                                    written: 0,
                                };
                            }
                            continue;
                        }
                        _ => {
                            me.state = WsReadState::Header { filled: 0 };
                            continue;
                        }
                    }
                }

                WsReadState::ReadingPing { remaining, payload } => {
                    let mut ping_buf = [0u8; 125];
                    let to_read = (*remaining).min(ping_buf.len());
                    let mut rb = ReadBuf::new(&mut ping_buf[..to_read]);
                    match Pin::new(&mut me.inner).poll_read(cx, &mut rb) {
                        Poll::Ready(Ok(())) => {
                            let n = rb.filled().len();
                            if n == 0 {
                                me.state = WsReadState::Closed;
                                return Poll::Ready(None);
                            }
                            payload.extend_from_slice(&ping_buf[..n]);
                            *remaining -= n;
                            if *remaining == 0 {
                                let mut pong = vec![0u8; 2 + payload.len()];
                                let pong_len = websocket::write_pong_frame(&mut pong, payload);
                                pong.truncate(pong_len);
                                me.state = WsReadState::SendingPong {
                                    buf: pong,
                                    written: 0,
                                };
                            }
                            continue;
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                        Poll::Pending => return Poll::Pending,
                    }
                }

                WsReadState::SendingPong { buf, written } => {
                    let mut guard = me.writer.lock().unwrap();
                    let remaining = &buf[*written..];
                    match Pin::new(&mut *guard).poll_write(cx, remaining) {
                        Poll::Ready(Ok(n)) => {
                            *written += n;
                            if *written >= buf.len() {
                                me.state = WsReadState::Header { filled: 0 };
                            }
                            continue;
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                        Poll::Pending => return Poll::Pending,
                    }
                }

                WsReadState::Payload {
                    remaining,
                    masked,
                    mask_key,
                    mask_offset,
                } => {
                    if *remaining == 0 {
                        me.state = WsReadState::Header { filled: 0 };
                        continue;
                    }

                    let chunk_start = me.arena.write_pos();
                    let n = {
                        let spare = me.arena.spare_mut();
                        let read_len = spare.len().min(*remaining);
                        let mut read_buf = ReadBuf::new(&mut spare[..read_len]);
                        match Pin::new(&mut me.inner).poll_read(cx, &mut read_buf) {
                            Poll::Ready(Ok(())) => read_buf.filled().len(),
                            Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                            Poll::Pending => return Poll::Pending,
                        }
                    };

                    if n == 0 {
                        me.state = WsReadState::Closed;
                        return Poll::Ready(None);
                    }

                    if *masked {
                        let spare = me.arena.spare_mut();
                        for i in 0..n {
                            spare[i] ^= mask_key[(*mask_offset + i) % 4];
                        }
                        *mask_offset = (*mask_offset + n) % 4;
                    }

                    me.arena.advance(n);
                    *remaining -= n;

                    if *remaining == 0 {
                        me.state = WsReadState::Header { filled: 0 };
                    }

                    let shared = me.arena.window(chunk_start, n);
                    me.arena.align();
                    return Poll::Ready(Some(Ok(shared)));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Write path
// ---------------------------------------------------------------------------

/// Write state for WebSocket binary framing.
enum WsWriteState {
    Idle,
    WritingHeader {
        header: [u8; MAX_HEADER_LEN],
        header_len: usize,
        header_written: usize,
        payload_total: usize,
    },
    WritingPayload {
        payload_total: usize,
        payload_written: usize,
    },
}

/// WebSocket write adapter over a raw TCP stream.
///
/// Wraps each `poll_write` call's bytes in a WS binary frame. The WS
/// header is written from a stack buffer, then the caller's payload
/// follows. No intermediate copies.
///
/// Designed for use with `write_all` where each call represents a
/// complete message.
pub struct WsWrite<W> {
    inner: Arc<Mutex<W>>,
    state: WsWriteState,
}

impl<W: AsyncWrite + Unpin> WsWrite<W> {
    /// Create a WebSocket write adapter from a raw TCP write half.
    ///
    /// Returns the shared writer reference needed by WsRead for pong
    /// responses, and the WsWrite adapter for application data.
    /// The WebSocket handshake must already be complete.
    pub fn new(inner: W) -> (Arc<Mutex<W>>, Self) {
        let shared = Arc::new(Mutex::new(inner));
        let writer = Self {
            inner: Arc::clone(&shared),
            state: WsWriteState::Idle,
        };
        (shared, writer)
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for WsWrite<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let me = self.get_mut();
        let mut guard = me.inner.lock().unwrap();

        loop {
            match &mut me.state {
                WsWriteState::Idle => {
                    let mut header = [0u8; MAX_HEADER_LEN];
                    let header_len = websocket::write_binary_header(&mut header, buf.len());
                    me.state = WsWriteState::WritingHeader {
                        header,
                        header_len,
                        header_written: 0,
                        payload_total: buf.len(),
                    };
                    continue;
                }

                WsWriteState::WritingHeader {
                    header,
                    header_len,
                    header_written,
                    payload_total,
                } => {
                    let remaining = &header[*header_written..*header_len];
                    match Pin::new(&mut *guard).poll_write(cx, remaining) {
                        Poll::Ready(Ok(n)) => {
                            *header_written += n;
                            if *header_written >= *header_len {
                                let total = *payload_total;
                                me.state = WsWriteState::WritingPayload {
                                    payload_total: total,
                                    payload_written: 0,
                                };
                                continue;
                            }
                            continue;
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Pending => return Poll::Pending,
                    }
                }

                WsWriteState::WritingPayload {
                    payload_total,
                    payload_written,
                } => {
                    let remaining = *payload_total - *payload_written;
                    let to_write =
                        &buf[*payload_written..(*payload_written + remaining).min(buf.len())];
                    match Pin::new(&mut *guard).poll_write(cx, to_write) {
                        Poll::Ready(Ok(n)) => {
                            *payload_written += n;
                            if *payload_written >= *payload_total {
                                let written = *payload_total;
                                me.state = WsWriteState::Idle;
                                return Poll::Ready(Ok(written));
                            }
                            continue;
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut guard = self.get_mut().inner.lock().unwrap();
        Pin::new(&mut *guard).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut guard = self.get_mut().inner.lock().unwrap();
        let mut close_buf = [0u8; MAX_HEADER_LEN];
        let n = websocket::write_close_frame(&mut close_buf);
        let _ = Pin::new(&mut *guard).poll_write(cx, &close_buf[..n]);
        Pin::new(&mut *guard).poll_shutdown(cx)
    }
}
