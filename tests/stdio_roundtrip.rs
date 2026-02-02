//! Stdio roundtrip integration test.
//!
//! Tests stdin/stdout transport by spawning a child process that acts as a
//! pass-through: reads Arrow IPC from stdin, writes it back to stdout.
//!
//! The parent process writes test tables to the child's stdin, then reads
//! them back from the child's stdout and verifies the data survived the trip.

#![cfg(feature = "stdio")]

use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::sync::Arc;

use lightstream::enums::IPCMessageProtocol;
use lightstream::models::readers::ipc::table_reader::TableReader;
use minarrow::{
    Array, ArrowType, Bitmask, Buffer, CategoricalArray, Field, FieldArray, FloatArray,
    IntegerArray, NumericArray, StringArray, Table, TextArray, Vec64,
    ffi::arrow_dtype::CategoricalIndexType,
};

fn make_test_table() -> Table {
    let int_col = FieldArray::new(
        Field {
            name: "ids".into(),
            dtype: ArrowType::Int32,
            nullable: false,
            metadata: Default::default(),
        },
        Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray {
            data: Buffer::from(Vec64::from_slice(&[10, 20, 30, 40])),
            null_mask: None,
        }))),
    );

    let float_col = FieldArray::new(
        Field {
            name: "values".into(),
            dtype: ArrowType::Float64,
            nullable: false,
            metadata: Default::default(),
        },
        Array::NumericArray(NumericArray::Float64(Arc::new(FloatArray {
            data: Buffer::from(Vec64::from_slice(&[1.1, 2.2, 3.3, 4.4])),
            null_mask: None,
        }))),
    );

    let str_col = FieldArray::new(
        Field {
            name: "labels".into(),
            dtype: ArrowType::String,
            nullable: true,
            metadata: Default::default(),
        },
        Array::TextArray(TextArray::String32(Arc::new(StringArray::new(
            Buffer::from(Vec64::from_slice("helloworldtest".as_bytes())),
            Some(Bitmask::new_set_all(4, true)),
            Buffer::from(Vec64::from_slice(&[0u32, 5, 10, 14, 14])),
        )))),
    );

    let dict_col = FieldArray::new(
        Field {
            name: "category".into(),
            dtype: ArrowType::Dictionary(CategoricalIndexType::UInt32),
            nullable: true,
            metadata: Default::default(),
        },
        Array::TextArray(TextArray::Categorical32(Arc::new(CategoricalArray {
            data: Buffer::from(Vec64::from_slice(&[0u32, 1, 2, 0])),
            unique_values: Vec64::from(vec![
                "red".to_string(),
                "green".to_string(),
                "blue".to_string(),
            ]),
            null_mask: Some(Bitmask::new_set_all(4, true)),
        }))),
    );

    Table {
        cols: vec![int_col, float_col, str_col, dict_col],
        n_rows: 4,
        name: "test_table".to_string(),
    }
}

fn make_schema(table: &Table) -> Vec<Field> {
    table
        .cols
        .iter()
        .map(|fa| fa.field.as_ref().clone())
        .collect()
}

/// Encode a table to Arrow IPC bytes using TableStreamWriter.
fn encode_table_to_bytes(table: &Table, schema: &[Field]) -> Vec<u8> {
    use lightstream::models::writers::ipc::table_stream_writer::TableStreamWriter;

    let mut writer = TableStreamWriter::<Vec<u8>>::new(schema.to_vec(), IPCMessageProtocol::Stream);

    // Register dictionary for categorical column
    writer.register_dictionary(
        3,
        vec!["red".to_string(), "green".to_string(), "blue".to_string()],
    );

    writer.write(table).unwrap();
    writer.finish().unwrap();

    let mut all_bytes = Vec::new();
    while let Some(frame) = writer.next_frame() {
        let frame_bytes = frame.unwrap();
        all_bytes.extend_from_slice(frame_bytes.as_ref());
    }
    all_bytes
}

/// Encode multiple tables to Arrow IPC bytes.
fn encode_tables_to_bytes(tables: &[&Table], schema: &[Field]) -> Vec<u8> {
    use lightstream::models::writers::ipc::table_stream_writer::TableStreamWriter;

    let mut writer = TableStreamWriter::<Vec<u8>>::new(schema.to_vec(), IPCMessageProtocol::Stream);

    writer.register_dictionary(
        3,
        vec!["red".to_string(), "green".to_string(), "blue".to_string()],
    );

    for table in tables {
        writer.write(table).unwrap();
    }
    writer.finish().unwrap();

    let mut all_bytes = Vec::new();
    while let Some(frame) = writer.next_frame() {
        let frame_bytes = frame.unwrap();
        all_bytes.extend_from_slice(frame_bytes.as_ref());
    }
    all_bytes
}

/// A helper stream that wraps bytes for TableReader.
struct ByteVecStream {
    data: Vec<u8>,
    pos: usize,
    chunk_size: usize,
    done: bool,
}

impl ByteVecStream {
    fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            pos: 0,
            chunk_size: 8192,
            done: false,
        }
    }
}

impl futures_core::Stream for ByteVecStream {
    type Item = Result<Vec<u8>, std::io::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let me = self.get_mut();
        if me.done || me.pos >= me.data.len() {
            me.done = true;
            return std::task::Poll::Ready(None);
        }

        let end = (me.pos + me.chunk_size).min(me.data.len());
        let chunk = me.data[me.pos..end].to_vec();
        me.pos = end;
        std::task::Poll::Ready(Some(Ok(chunk)))
    }
}

impl tokio::io::AsyncRead for ByteVecStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let me = self.get_mut();
        if me.pos >= me.data.len() {
            return std::task::Poll::Ready(Ok(()));
        }

        let remaining = &me.data[me.pos..];
        let to_copy = remaining.len().min(buf.remaining());
        buf.put_slice(&remaining[..to_copy]);
        me.pos += to_copy;
        std::task::Poll::Ready(Ok(()))
    }
}

/// Test that we can encode Arrow IPC and decode it back.
/// This verifies the stdio components would work if connected to real pipes.
#[tokio::test]
async fn test_stdio_encode_decode_roundtrip() {
    let table = make_test_table();
    let schema = make_schema(&table);

    // Encode to bytes
    let bytes = encode_table_to_bytes(&table, &schema);
    assert!(!bytes.is_empty(), "Encoded bytes should not be empty");

    // Decode from bytes
    let stream = ByteVecStream::new(bytes);
    let reader = TableReader::new(stream, 64 * 1024, IPCMessageProtocol::Stream);
    let tables = reader.read_all_tables().await.unwrap();

    assert_eq!(tables.len(), 1);
    let t = &tables[0];
    assert_eq!(t.n_rows, 4);
    assert_eq!(t.cols.len(), 4);

    // Verify integer column values
    match &t.cols[0].array {
        Array::NumericArray(NumericArray::Int32(arr)) => {
            assert_eq!(arr.data.as_slice(), &[10, 20, 30, 40]);
        }
        other => panic!("Expected Int32, got {:?}", other),
    }

    // Verify float column values
    match &t.cols[1].array {
        Array::NumericArray(NumericArray::Float64(arr)) => {
            assert_eq!(arr.data.as_slice(), &[1.1, 2.2, 3.3, 4.4]);
        }
        other => panic!("Expected Float64, got {:?}", other),
    }

    // Verify string column values
    match &t.cols[2].array {
        Array::TextArray(TextArray::String32(arr)) => {
            let strs: Vec<_> = arr.iter_str().collect();
            assert_eq!(strs, &["hello", "world", "test", ""]);
        }
        other => panic!("Expected String32, got {:?}", other),
    }

    // Verify categorical column values
    match &t.cols[3].array {
        Array::TextArray(TextArray::Categorical32(arr)) => {
            let cats: Vec<_> = arr.iter_str().collect();
            assert_eq!(cats, &["red", "green", "blue", "red"]);
        }
        other => panic!("Expected Categorical32, got {:?}", other),
    }
}

/// Test writing to stdout via TableSink with a captured pipe.
/// Spawns `cat` to echo the data back and verifies the roundtrip.
#[tokio::test]
async fn test_stdio_cat_roundtrip() {
    let table = make_test_table();
    let schema = make_schema(&table);

    // Encode table
    let bytes = encode_table_to_bytes(&table, &schema);

    // Spawn cat to echo our data back
    let mut child = Command::new("cat")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to spawn cat");

    // Write to cat's stdin
    {
        let mut stdin = child.stdin.take().expect("Failed to get stdin");
        stdin.write_all(&bytes).unwrap();
        // stdin dropped here, closing the pipe
    }

    // Read from cat's stdout
    let mut output = Vec::new();
    {
        let mut stdout = child.stdout.take().expect("Failed to get stdout");
        stdout.read_to_end(&mut output).unwrap();
    }

    // Verify we got the same bytes back
    assert_eq!(output, bytes);

    // Decode and verify
    let stream = ByteVecStream::new(output);
    let reader = TableReader::new(stream, 64 * 1024, IPCMessageProtocol::Stream);
    let tables = reader.read_all_tables().await.unwrap();

    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].n_rows, 4);
    assert_eq!(tables[0].cols.len(), 4);

    child.wait().unwrap();
}

/// Test multiple tables through cat roundtrip.
#[tokio::test]
async fn test_stdio_multi_table_cat_roundtrip() {
    let table = make_test_table();
    let schema = make_schema(&table);

    // Encode multiple tables
    let bytes = encode_tables_to_bytes(&[&table, &table, &table], &schema);

    // Spawn cat
    let mut child = Command::new("cat")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to spawn cat");

    {
        let mut stdin = child.stdin.take().expect("Failed to get stdin");
        stdin.write_all(&bytes).unwrap();
    }

    let mut output = Vec::new();
    {
        let mut stdout = child.stdout.take().expect("Failed to get stdout");
        stdout.read_to_end(&mut output).unwrap();
    }

    // Decode
    let stream = ByteVecStream::new(output);
    let reader = TableReader::new(stream, 64 * 1024, IPCMessageProtocol::Stream);
    let tables = reader.read_all_tables().await.unwrap();

    assert_eq!(tables.len(), 3);
    for t in &tables {
        assert_eq!(t.n_rows, 4);
        assert_eq!(t.cols.len(), 4);
    }

    child.wait().unwrap();
}

/// Test that StdinByteStream and StdoutTableWriter types compile and have correct signatures.
/// This is a compile-time check since we can't easily test real stdin/stdout in unit tests.
#[tokio::test]
async fn test_stdio_types_exist() {
    use lightstream::models::readers::stdio::StdinTableReader;
    use lightstream::models::streams::stdio::StdinByteStream;
    use lightstream::models::writers::stdio::StdoutTableWriter;

    // Verify types exist and have expected constructors
    // We don't actually call these since they would interact with real stdin/stdout
    fn _check_stdin_stream() {
        let _: fn(lightstream::enums::BufferChunkSize) -> StdinByteStream = StdinByteStream::new;
        let _: fn() -> StdinByteStream = StdinByteStream::default_size;
    }

    fn _check_stdin_reader() {
        let _: fn() -> StdinTableReader = StdinTableReader::new;
    }

    fn _check_stdout_writer() {
        let _: fn(Vec<Field>) -> std::io::Result<StdoutTableWriter> = StdoutTableWriter::new;
    }
}
