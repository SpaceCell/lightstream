//! # WebSocket byte stream and sink adapters
//!
//! Converts a WebSocket connection into the byte-level interfaces used by
//! Lightstream's Arrow IPC pipeline.
//!
//! ## Read path — [`WebSocketByteStream`]
//!
//! Wraps any `Stream<Item = Result<Message, tungstenite::Error>>` and yields
//! binary message payloads as `Vec<u8>` chunks. Text, Ping, Pong, and Frame
//! messages are silently skipped; a Close message terminates the stream.
//!
//! Also implements [`AsyncRead`] for compatibility with framed decoders that
//! consume bytes directly.
//!
//! ## Write path — [`WebSocketSinkAdapter`]
//!
//! Wraps a WebSocket `Sink<Message>` and implements [`AsyncWrite`], sending
//! each write as a binary WebSocket message. Shutdown sends a Close frame.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::tungstenite;
use tungstenite::{Bytes, Message};

/// A `Stream` that extracts binary payloads from WebSocket messages.
///
/// WebSocket is message-oriented, so each binary message becomes one
/// `Vec<u8>` item. Non-binary messages are skipped. A Close message
/// or stream termination sets the end-of-stream flag.
///
/// Alignment to 64 bytes is deferred to the Arrow decoding layer where
/// it matters — network transport payloads are not aligned on the wire.
///
/// Also implements `AsyncRead` by buffering the current message and
/// draining it across successive reads.
pub struct WebSocketByteStream<S> {
    /// The underlying WebSocket message stream.
    stream: S,
    /// End-of-stream flag.
    eof: bool,
    /// Residual bytes from a partially consumed message for `AsyncRead`.
    pending: Vec<u8>,
    /// Current read position within `pending`.
    pending_pos: usize,
}

impl<S> WebSocketByteStream<S>
where
    S: Stream<Item = Result<Message, tungstenite::Error>> + Unpin + Send + Sync,
{
    /// Wrap a WebSocket message stream.
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            eof: false,
            pending: Vec::new(),
            pending_pos: 0,
        }
    }

    /// Poll the inner stream for the next binary payload, skipping
    /// non-binary messages. Returns `None` on Close or stream end.
    fn poll_next_binary(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Vec<u8>, io::Error>>> {
        loop {
            if self.eof {
                return Poll::Ready(None);
            }

            let item = futures_core::ready!(Pin::new(&mut self.stream).poll_next(cx));

            match item {
                Some(Ok(Message::Binary(bytes))) => {
                    return Poll::Ready(Some(Ok(bytes.to_vec())));
                }
                Some(Ok(Message::Close(_))) => {
                    self.eof = true;
                    return Poll::Ready(None);
                }
                // Skip text, ping, pong, and raw frame messages
                Some(Ok(_)) => continue,
                Some(Err(e)) => {
                    self.eof = true;
                    return Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        e,
                    ))));
                }
                None => {
                    self.eof = true;
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl<S> Stream for WebSocketByteStream<S>
where
    S: Stream<Item = Result<Message, tungstenite::Error>> + Unpin + Send + Sync,
{
    type Item = Result<Vec<u8>, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        me.poll_next_binary(cx)
    }
}

impl<S> AsyncRead for WebSocketByteStream<S>
where
    S: Stream<Item = Result<Message, tungstenite::Error>> + Unpin + Send + Sync,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();

        // Drain any residual bytes from the previous message first
        if me.pending_pos < me.pending.len() {
            let remaining = &me.pending[me.pending_pos..];
            let n = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..n]);
            me.pending_pos += n;
            if me.pending_pos >= me.pending.len() {
                me.pending.clear();
                me.pending_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        // Fetch the next binary message
        match me.poll_next_binary(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                let n = bytes.len().min(buf.remaining());
                buf.put_slice(&bytes[..n]);
                if n < bytes.len() {
                    me.pending = bytes;
                    me.pending_pos = n;
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(None) => Poll::Ready(Ok(())), // EOF — zero bytes read
            Poll::Pending => Poll::Pending,
        }
    }
}

// ---------------------------------------------------------------------------
// Write path
// ---------------------------------------------------------------------------

/// An `AsyncWrite` adapter that sends bytes as binary WebSocket messages.
///
/// Each `poll_write` call wraps the provided bytes in a `Message::Binary`
/// and sends it through the underlying WebSocket sink. `poll_shutdown`
/// sends a Close frame before flushing.
pub struct WebSocketSinkAdapter<K> {
    /// The underlying WebSocket sink half.
    sink: K,
}

impl<K> WebSocketSinkAdapter<K>
where
    K: futures_sink::Sink<Message, Error = tungstenite::Error> + Unpin + Send + Sync,
{
    /// Wrap a WebSocket sink half as an `AsyncWrite`.
    pub fn new(sink: K) -> Self {
        Self { sink }
    }
}

impl<K> AsyncWrite for WebSocketSinkAdapter<K>
where
    K: futures_sink::Sink<Message, Error = tungstenite::Error> + Unpin + Send + Sync,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let me = self.get_mut();

        // Ensure the sink is ready to accept a message
        match Pin::new(&mut me.sink).poll_ready(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => {
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)));
            }
            Poll::Pending => return Poll::Pending,
        }

        let msg = Message::Binary(Bytes::copy_from_slice(buf));
        match Pin::new(&mut me.sink).start_send(msg) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(e) => Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        Pin::new(&mut me.sink)
            .poll_flush(cx)
            .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let me = self.get_mut();

        // Send a close frame, then flush
        match Pin::new(&mut me.sink).poll_ready(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => {
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)));
            }
            Poll::Pending => return Poll::Pending,
        }

        let _ = Pin::new(&mut me.sink).start_send(Message::Close(None));

        Pin::new(&mut me.sink)
            .poll_close(cx)
            .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))
    }
}
