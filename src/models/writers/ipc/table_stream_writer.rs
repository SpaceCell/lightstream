//! # Synchronous Arrow IPC Table Writer
//!
//! Provides a streaming, synchronous encoder for writing [`minarrow::Table`] values
//! into Arrow IPC frames - File or Stream protocol
//!
//! ## Overview
//! - Wraps the generic [`GTableStreamEncoder`] for frame emission
//! - For pipes, custom network protocols, or synchronous contexts
//! - Emits schema, dictionaries, record batches, and end-of-stream/footer
//! - Frames can be pulled incrementally (`next_frame`) or drained all at once
//!
//! ## Async Helpers
//! - [`write_tables_to_stream`] – write a sequence of tables to an async sink.
//! - [`write_table_to_stream`] – write a single table to an async sink.
//!
//! ## Usage
//! ```ignore
//! let mut writer = TableStreamWriter::<Vec<u8>>::new(schema, IPCMessageProtocol::Stream);
//! writer.register_dictionary(0, vec!["A".into(), "B".into()]);
//! writer.write(&table)?;
//! writer.finish()?;
//! while let Some(frame) = writer.next_frame() {
//!     let buf = frame?;
//!     sink.write_all(buf.as_ref()).await?;
//! }
//! ```

use crate::enums::IPCMessageProtocol;
use crate::models::encoders::ipc::table_stream::GTableStreamEncoder;
use crate::traits::stream_buffer::StreamBuffer;
use crate::utils::extract_dictionary_values_from_col;
use futures_core::Stream;
use minarrow::{Field, Table};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

/// # Synchronous Arrow IPC Table Writer
///
/// Encodes [`minarrow::Table`] values into Arrow File or Stream protocol IPC frames.
/// Great for pipes, custom transports, or synchronous contexts.
///
/// ## Example
/// ```ignore
/// let mut writer = TableStreamWriter::<Vec<u8>>::new(schema, IPCMessageProtocol::Stream);
/// writer.register_dictionary(0, vec!["A".into(), "B".into()]);
/// writer.write(&table)?;
/// writer.finish()?;
/// while let Some(frame) = writer.next_frame() {
///     let buf = frame?;
///     sink.write_all(buf.as_ref()).await?;
/// }
/// ```
pub struct TableStreamWriter<B>
where
    B: StreamBuffer + Unpin + 'static,
{
    encoder: GTableStreamEncoder<B>,
}

impl<B> TableStreamWriter<B>
where
    B: StreamBuffer + Unpin + 'static,
{
    /// Create a new streaming Arrow Table writer with the given schema and protocol.
    pub fn new(schema: Vec<Field>, protocol: IPCMessageProtocol) -> Self {
        Self {
            encoder: GTableStreamEncoder::<B>::new(schema, protocol),
        }
    }

    /// Register a dictionary (for categorical columns).
    pub fn register_dictionary(&mut self, dict_id: i64, values: Vec<String>) {
        self.encoder.register_dictionary(dict_id, values);
    }

    /// Write a single Table as a record batch frame.
    /// Emits schema and any required dictionaries as needed.
    pub fn write(&mut self, table: &Table) -> io::Result<()> {
        self.encoder.write_record_batch_frame(table)
    }

    /// Emit Arrow footer/EOS marker, finalising the stream.
    /// This must be called before draining all frames.
    pub fn finish(&mut self) -> io::Result<()> {
        self.encoder.finish()
    }

    /// Poll the next encoded Arrow IPC frame (as a buffer chunk).
    /// Returns None when all frames are emitted and finished.
    pub fn next_frame(&mut self) -> Option<io::Result<B>> {
        self.encoder.out_frames.pop_front().map(Ok)
    }

    /// Drain all remaining encoded frames to a Vec.
    /// This is a utility for "all-at-once" use cases (e.g., tests).
    pub fn drain_all_frames(&mut self) -> Vec<B> {
        let mut out = Vec::new();
        while let Some(frame) = self.encoder.out_frames.pop_front() {
            out.push(frame);
        }
        out
    }

    /// Return true if the stream is finished (no more frames).
    pub fn is_finished(&self) -> bool {
        self.encoder.finished && self.encoder.out_frames.is_empty()
    }

    /// Access current writer schema.
    pub fn schema(&self) -> &[Field] {
        &self.encoder.schema
    }
}

impl<B> Stream for TableStreamWriter<B>
where
    B: StreamBuffer + Unpin + 'static,
{
    type Item = io::Result<B>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(frame) = this.encoder.out_frames.pop_front() {
            Poll::Ready(Some(Ok(frame)))
        } else if this.encoder.finished {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

/// Write a sequence of `Table`s to an arbitrary async stream (socket, pipe, etc.).
///
/// * `stream`      – the destination async byte sink
/// * `tables`      – the batches to write (each a `Table`)
/// * `schema`      – the common schema (must match each `Table`)
/// * `protocol`    – IPC protocol to use (File or Stream)
pub async fn write_tables_to_stream<W, B>(
    mut stream: W,
    tables: &[Table],
    schema: Vec<Field>,
    protocol: IPCMessageProtocol,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin + Send + Sync,
    B: StreamBuffer + Unpin,
{
    let mut writer = TableStreamWriter::<B>::new(schema, protocol);

    for table in tables {
        for (col_idx, col) in table.cols.iter().enumerate() {
            if let Some(values) = extract_dictionary_values_from_col(col) {
                writer.register_dictionary(col_idx as i64, values);
            }
        }
        writer.write(table)?;
    }
    writer.finish()?;

    while let Some(frame) = writer.next_frame() {
        let buf = frame?;
        stream.write_all(buf.as_ref()).await?;
    }
    stream.flush().await?;
    Ok(())
}

/// Write a single `Table` to an arbitrary async stream (socket, pipe, etc.).
///
/// * `stream`      – the destination async byte sink
/// * `table`       – the batch to write (a `Table`)
/// * `schema`      – the schema (must match the table)
/// * `protocol`    – IPC protocol to use (File or Stream)
pub async fn write_table_to_stream<W, B>(
    mut stream: W,
    table: &Table,
    schema: Vec<Field>,
    protocol: IPCMessageProtocol,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin + Send + Sync,
    B: StreamBuffer + Unpin,
{
    let mut writer = TableStreamWriter::<B>::new(schema, protocol);

    // Register dictionaries (if any categorical columns present)
    for (col_idx, col) in table.cols.iter().enumerate() {
        if let Some(values) = extract_dictionary_values_from_col(col) {
            writer.register_dictionary(col_idx as i64, values);
        }
    }
    writer.write(table)?;
    writer.finish()?;

    while let Some(frame) = writer.next_frame() {
        let buf = frame?;
        stream.write_all(buf.as_ref()).await?;
    }
    stream.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enums::IPCMessageProtocol;
    use crate::test_helpers::*;
    use minarrow::{Field, Table, Vec64};
    use std::io;

    fn all_types_schema() -> Vec<Field> {
        make_schema_all_types()
    }

    fn test_table() -> Table {
        make_all_types_table()
    }

    #[test]
    fn test_table_stream_writer_schema_and_finish() {
        let schema = all_types_schema();
        let mut writer =
            TableStreamWriter::<Vec64<u8>>::new(schema.clone(), IPCMessageProtocol::Stream);
        assert_eq!(writer.schema(), &schema[..]);
        assert!(!writer.is_finished());
        writer.finish().unwrap();
        assert!(writer.is_finished());
    }

    #[test]
    fn test_write_and_drain_one_table() {
        let schema = all_types_schema();
        let table = test_table();

        let mut writer =
            TableStreamWriter::<Vec64<u8>>::new(schema.clone(), IPCMessageProtocol::Stream);
        // Register dictionaries for categorical columns
        for (col_idx, col) in table.cols.iter().enumerate() {
            if let Some(values) = extract_dictionary_values_from_col(col) {
                writer.register_dictionary(col_idx as i64, values);
            }
        }
        writer.write(&table).unwrap();
        writer.finish().unwrap();

        let frames = writer.drain_all_frames();
        assert!(
            !frames.is_empty(),
            "No frames emitted after writing table and finish"
        );

        // The first frame is the schema, at least one record batch frame, and an EOS marker.
        assert!(frames.len() >= 2);
        let total_len: usize = frames.iter().map(|f| f.len()).sum();
        assert!(total_len > 0);
    }

    #[test]
    fn test_multiple_batches_emit_multiple_frames() {
        let schema = all_types_schema();
        let table1 = test_table();
        let mut table2 = test_table();
        table2.name = "another".into();

        let mut writer =
            TableStreamWriter::<Vec64<u8>>::new(schema.clone(), IPCMessageProtocol::Stream);
        // Register dictionaries for categorical columns
        for (col_idx, col) in table1.cols.iter().enumerate() {
            if let Some(values) = extract_dictionary_values_from_col(col) {
                writer.register_dictionary(col_idx as i64, values);
            }
        }
        writer.write(&table1).unwrap();
        writer.write(&table2).unwrap();
        writer.finish().unwrap();

        let frames = writer.drain_all_frames();
        // At least: 1 schema + 2 batches + 1 EOS
        assert!(
            frames.len() >= 4,
            "Expected at least 4 frames: schema, 2 batches, EOS"
        );
    }

    #[test]
    fn test_next_frame_returns_none_when_empty() {
        let schema = all_types_schema();
        let mut writer = TableStreamWriter::<Vec64<u8>>::new(schema, IPCMessageProtocol::Stream);
        assert!(writer.next_frame().is_none());
        writer.finish().unwrap();
        assert!(writer.next_frame().is_none());
    }

    #[test]
    fn test_error_on_schema_mismatch() {
        let schema = all_types_schema();
        let mut bad_table = test_table();
        bad_table.cols.pop(); // Now schema and columns mismatch
        let mut writer = TableStreamWriter::<Vec64<u8>>::new(schema, IPCMessageProtocol::Stream);
        let err = writer.write(&bad_table).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    // #[test]
    // fn test_dictionary_column() {
    //     let mut table = test_table();
    //     // Add a dictionary column if not present
    //     let mut schema = all_types_schema();
    //     let dict_col = dict32_col();
    //     schema.push(dict_col.field.as_ref().clone());
    //     table.cols.push(dict_col.clone());

    //     let mut writer = TableStreamWriter::<Vec64<u8>>::new(schema.clone(), IPCMessageProtocol::Stream);
    //     // Register dictionary explicitly
    //     writer.register_dictionary((table.cols.len() - 1) as i64, dict_col.array.as_dict_values().unwrap());
    //     writer.write(&table).unwrap();
    //     writer.finish().unwrap();

    //     let frames = writer.drain_all_frames();
    //     assert!(frames.len() >= 3, "Should emit schema, dictionary, batch, EOS");
    // }

    #[test]
    fn test_stream_trait_polling() {
        let schema = all_types_schema();
        let table = test_table();

        let mut writer = TableStreamWriter::<Vec64<u8>>::new(schema, IPCMessageProtocol::Stream);
        // Register dictionaries for categorical columns
        for (col_idx, col) in table.cols.iter().enumerate() {
            if let Some(values) = extract_dictionary_values_from_col(col) {
                writer.register_dictionary(col_idx as i64, values);
            }
        }
        writer.write(&table).unwrap();
        writer.finish().unwrap();

        let mut pin_writer = Box::pin(writer);
        let mut frames = Vec::new();
        let cx = futures_util::task::noop_waker_ref();

        // Manual poll
        loop {
            match Pin::new(&mut pin_writer)
                .as_mut()
                .poll_next(&mut Context::from_waker(cx))
            {
                Poll::Ready(Some(Ok(frame))) => frames.push(frame),
                Poll::Ready(None) => break,
                Poll::Ready(Some(Err(e))) => panic!("Unexpected error from poll_next: {e}"),
                Poll::Pending => continue,
            }
        }
        assert!(
            !frames.is_empty(),
            "Should emit at least some frames through poll_next"
        );
    }
}
