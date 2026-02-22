//! Async Lightstream protocol writer.
//!
//! Wraps a [`LightstreamCodec`] and an [`AsyncWrite`] destination, providing
//! methods to send messages and Arrow tables over a single connection.
//!
//! Tables are encoded using the Arrow IPC streaming protocol — schema is
//! sent once per table type, then only record batches after that.
//!
//! With the `protobuf` feature, [`send_proto`] provides typed message
//! sending via prost. With the `msgpack` feature, [`send_msgpack`]
//! provides typed message sending via MessagePack and serde.
//!
//! [`send_proto`]: LightstreamWriter::send_proto
//! [`send_msgpack`]: LightstreamWriter::send_msgpack

use std::io;

use minarrow::Field;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::models::protocol::codec::LightstreamCodec;
use crate::traits::stream_buffer::StreamBuffer;

/// Async writer for the Lightstream protocol.
///
/// Encodes messages and Arrow tables via the codec and writes the
/// resulting frames to the underlying `AsyncWrite` destination.
/// Table payloads use the Arrow IPC streaming protocol internally.
pub struct LightstreamWriter<W: AsyncWrite + Unpin + Send, B: StreamBuffer + Unpin = Vec<u8>> {
    codec: LightstreamCodec<B>,
    dest: W,
}

impl<W: AsyncWrite + Unpin + Send, B: StreamBuffer + Unpin> LightstreamWriter<W, B> {
    /// Create a new writer over the given destination.
    pub fn new(dest: W) -> Self {
        Self {
            codec: LightstreamCodec::new(),
            dest,
        }
    }

    /// Register a message type. Returns the assigned type tag.
    pub fn register_message(&mut self, name: impl Into<String>) -> u8 {
        self.codec.register_message(name)
    }

    /// Register a table type with the given schema. Returns the assigned type tag.
    pub fn register_table(&mut self, name: impl Into<String>, schema: Vec<Field>) -> u8 {
        self.codec.register_table(name, schema)
    }

    /// Send an opaque message payload by type name.
    pub async fn send(&mut self, name: &str, payload: &[u8]) -> io::Result<()> {
        let tag = self.codec.tag_by_name(name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown type name '{}'", name),
            )
        })?;
        let frame = self.codec.encode_message(tag, payload)?;
        self.dest.write_all(frame.as_ref()).await?;
        Ok(())
    }

    /// Send an Arrow table by type name.
    pub async fn send_table(&mut self, name: &str, table: &minarrow::Table) -> io::Result<()> {
        let tag = self.codec.tag_by_name(name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown type name '{}'", name),
            )
        })?;
        let frame = self.codec.encode_table(tag, table)?;
        self.dest.write_all(frame.as_ref()).await?;
        Ok(())
    }

    /// Flush the underlying writer.
    pub async fn flush(&mut self) -> io::Result<()> {
        self.dest.flush().await
    }

    /// Shut down the underlying writer.
    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.dest.shutdown().await
    }

    /// Send a protobuf message by type name.
    ///
    /// Encodes the message via `prost::Message::encode_to_vec` and sends it
    /// as an opaque payload.
    #[cfg(feature = "protobuf")]
    pub async fn send_proto<M: prost::Message>(
        &mut self,
        name: &str,
        msg: &M,
    ) -> io::Result<()> {
        let bytes = msg.encode_to_vec();
        self.send(name, &bytes).await
    }

    /// Send a MessagePack-encoded message by type name.
    ///
    /// Serialises the value via `rmp-serde` with `BytesMode::ForceAll` so
    /// that `Vec<u8>` and `&[u8]` fields are stored as binary, not as
    /// arrays of integers.
    #[cfg(feature = "msgpack")]
    pub async fn send_msgpack<M: serde::Serialize>(
        &mut self,
        name: &str,
        msg: &M,
    ) -> io::Result<()> {
        let bytes = encode_msgpack(msg)?;
        self.send(name, &bytes).await
    }

    /// Borrow the codec for inspection.
    pub fn codec(&self) -> &LightstreamCodec<B> {
        &self.codec
    }
}

/// Encode a value to MessagePack bytes with efficient binary handling.
#[cfg(feature = "msgpack")]
fn encode_msgpack<M: serde::Serialize>(msg: &M) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut serializer = rmp_serde::Serializer::new(&mut buf)
        .with_bytes(rmp_serde::config::BytesMode::ForceAll);
    msg.serialize(&mut serializer)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(buf)
}
