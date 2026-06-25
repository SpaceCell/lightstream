//! # Lightstream
//!
//! Lightstream provides format codecs, file readers and writers, framed streams,
//! and network transports for Arrow data via [`minarrow`](https://crates.io/crates/minarrow)
//! tables.
//!
//! The crate supports both low-level composition and complete table-oriented I/O.
//! Encoders, decoders, framing, byte streams and transports are exposed
//! independently, while the reader and writer modules combine them into
//! higher-level interfaces.
//!
//! ## Formats and protocols
//!
//! - Arrow IPC stream and file formats
//! - Lightstream framed protocol
//! - TLV framing
//! - CSV
//! - JSON arrays and NDJSON
//! - Parquet
//! - Chunked Arrow IPC, CSV and Parquet datasets
//!
//! Format support includes synchronous and asynchronous readers and writers,
//! one-shot codecs, schema-aware decoding, dictionary-encoded columns and
//! configurable decode limits for untrusted input.
//!
//! ## Storage I/O
//!
//! - Arrow IPC file and stream readers and writers
//! - Memory-mapped Arrow IPC reads
//! - Chunked-directory readers and writers
//! - Serial and parallel chunk loading
//! - Disk-backed asynchronous byte streams
//! - Standard input and output
//!
//! Buffers use Minarrow's 64-byte-aligned storage. Owned decode
//! paths may reuse aligned input buffers without copying when the selected codec
//! permits it.
//!
//! ## Network transports
//!
//! - TCP
//! - Unix domain sockets
//! - WebSocket
//! - QUIC
//! - WebTransport
//! - HTTP/2 streaming requests and responses
//! - Linux `io_uring` transport support
//!
//! QUIC and HTTP/2 also provide parallel readers and writers that distribute
//! tables across several concurrent streams on one connection. The transport
//! interfaces are independent of the table codec, allowing the same framing and
//! encoding layers to be used with different transports.
//!
//! ## Compression
//!
//! Compression support is feature-gated and includes zstd and Snappy where
//! supported by the selected format or protocol.
//!
//! ## Feature flags
//!
//! Most optional formats and transports are controlled through Cargo features,
//! including:
//!
//! - `csv`
//! - `json`
//! - `parquet`
//! - `mmap`
//! - `tcp`
//! - `uds`
//! - `stdio`
//! - `websocket`
//! - `quic`
//! - `webtransport`
//! - `http`
//! - `protocol`
//! - `io_uring`
//! - `zstd`
//! - `snappy`
//!
//! ## Example: writing an Arrow IPC file
//!
//! ```rust,no_run
//! use lightstream::enums::IPCMessageProtocol;
//! use lightstream::models::writers::ipc::table::TableWriter;
//! use minarrow::{FieldArray, Table, arr_i32, arr_str32};
//! use tokio::fs::File;
//!
//! # async fn write() -> std::io::Result<()> {
//! let ids = FieldArray::from_arr("ids", arr_i32![1, 2, 3]);
//! let names = FieldArray::from_arr("names", arr_str32!["a", "b", "c"]);
//! let table = Table::new("example".to_string(), vec![ids, names].into());
//!
//! let file = File::create("out.arrow").await?;
//! let mut writer =
//!     TableWriter::from_schema(file, table.schema(), IPCMessageProtocol::File)?;
//!
//! writer.write_table(table).await?;
//! writer.finish().await?;
//! # Ok(())
//! # }
//! ```
//!
//! See the [project README](https://github.com/SpaceCell/lightstream) for build
//! configuration and further examples.


/// Composable traits for streaming bytes and frames.
pub mod traits {
    /// Chunked byte stream trait
    pub mod byte_stream;

    /// Pull-based frame decoder interface.
    pub mod frame_decoder;

    /// Push-based frame encoder interface.
    pub mod frame_encoder;

    /// Output buffer abstraction (`Vec<u8>`, `Vec64<u8>`, etc.).
    pub mod stream_buffer;

    /// Transport-level table reader trait
    pub mod transport_reader;

    /// Transport-level table writer trait
    pub mod transport_writer;

    /// Parallel transport reader trait - merges several concurrent
    /// streams on one connection into a single table stream.
    pub mod parallel_transport_reader;

    /// Parallel transport writer trait - fans a table sequence across
    /// several concurrent streams on one connection.
    pub mod parallel_transport_writer;

    /// Chunked-file table reader trait shared by the per-format chunked
    /// readers (`ChunkedCsvReader`, `ChunkedParquetReader`, `ChunkedArrowReader`).
    pub mod chunked_table_reader;

    /// Chunked-file table writer trait shared by the per-format chunked
    /// writers (`ChunkedCsvWriter`, `ChunkedParquetWriter`, `ChunkedArrowWriter`).
    pub mod chunked_table_writer;

    /// Type-level serialise/deserialise round-trip, parametrised over
    /// a format marker. Implemented ON minarrow value types.
    pub mod serialise;

    /// One-shot byte encoding contract for items in `models/encoders/`
    /// and `models/codecs/`.
    pub mod encoder;

    /// One-shot byte decoding contract for items in `models/decoders/`
    /// and `models/codecs/`.
    pub mod decoder;
}

/// Codec implementations, readers, writers, and I/O models
pub mod models {

    /// Sinks convert tables or TLV frames into byte streams
    pub mod sinks {
        /// Arrow IPC sink - Stream/File protocols
        pub mod table_sink;

        /// TLV sink for simple type-length-value framing
        pub mod tlv_sink;

        /// Live LBuffer-backed table sink for decoded records
        #[cfg(all(feature = "lbuffer", feature = "json"))]
        pub mod live_table_sink;
    }

    /// Encoders for Arrow IPC, TLV, CSV, and optionally Parquet
    pub mod encoders {
        /// Arrow IPC encoders
        pub mod ipc;

        /// Parquet encoders (if `parquet` feature is enabled)
        #[cfg(feature = "parquet")]
        pub mod parquet {
            /// Low-level value encoders
            pub mod data;

            /// Page and metadata writers
            pub mod metadata;
        }

        /// TLV wire format encoders
        pub mod tlv;

        /// CSV encoder for tables and supertables
        #[cfg(feature = "csv")]
        pub mod csv;

        /// JSON encoder for tables and supertables (array-of-objects / NDJSON)
        #[cfg(feature = "json")]
        pub mod json;

        /// Integer-to-decimal-ASCII formatter used by the CSV and JSON encoders.
        #[cfg(any(feature = "csv", feature = "json"))]
        mod int_ascii;
    }

    /// Decoders for Arrow IPC, CSV, TLV, and optionally Parquet
    pub mod decoders {
        /// Resource caps applied during decode of untrusted input.
        pub mod limits;

        /// Arrow IPC decoders
        pub mod ipc;

        /// CSV-to-table decoder
        #[cfg(feature = "csv")]
        pub mod csv;

        /// JSON-to-table decoder (array-of-objects / NDJSON)
        #[cfg(feature = "json")]
        pub mod json;

        /// Parquet decoder (if `parquet` feature is enabled)
        #[cfg(feature = "parquet")]
        pub mod parquet;

        /// TLV stream decoder
        pub mod tlv;

        /// ASCII-to-integer parser used by the CSV decoder.
        #[cfg(feature = "csv")]
        mod int_ascii;
    }

    /// Frame structures for IPC, TLV, and WebSocket
    pub mod frames {
        /// IPC message wrappers
        pub mod ipc_message;

        /// TLV frame definitions
        pub mod tlv_frame;

        /// Lightstream protocol message types
        #[cfg(feature = "protocol")]
        pub mod lightstream_message;

        /// WebSocket binary frame header parsing and unmasking
        #[cfg(feature = "websocket")]
        pub mod websocket;

        /// JSON frame and record cursors yielded by the JSON interface.
        #[cfg(feature = "json")]
        pub mod json;
    }

    /// Interface adapters mapping vendor wire shapes onto declared schemas.
    pub mod interfaces;

    /// Readers for files, mmap, and async streams
    pub mod readers {
        /// Arrow IPC readers
        pub mod ipc {
            /// File-based IPC reader
            pub mod file_table;

            /// 64-Byte Aligned Zero Copy Mmap IPC reader
            #[cfg(feature = "mmap")]
            pub mod mmap_table;

            /// Streamed IPC table reader
            pub mod table;
        }

        /// CSV reader utilities.
        #[cfg(feature = "csv")]
        pub mod csv;

        /// JSON reader (array-of-objects and NDJSON)
        #[cfg(feature = "json")]
        pub mod json;

        /// Parquet reader
        #[cfg(feature = "parquet")]
        pub mod parquet;

        /// Chunked file readers - glob a directory of `<base>-NNNNN.<ext>`
        /// files and present them as an ordered iterator of `Table`s.
        pub mod chunked {
            /// Chunked CSV reader.
            #[cfg(feature = "csv")]
            pub mod csv;

            /// Chunked Parquet reader.
            #[cfg(feature = "parquet")]
            pub mod parquet;

            /// Chunked Arrow IPC reader.
            pub mod arrow;
        }

        /// TCP table reader
        #[cfg(feature = "tcp")]
        pub mod tcp;

        /// WebSocket table reader
        #[cfg(feature = "websocket")]
        pub mod websocket;

        /// QUIC table reader
        #[cfg(feature = "quic")]
        pub mod quic;

        /// WebTransport table reader
        #[cfg(feature = "webtransport")]
        pub mod webtransport;

        /// HTTP/2 table reader (GET; streaming response body)
        #[cfg(feature = "http")]
        pub mod http;

        /// Parallel transport readers - merge several concurrent streams
        /// on one connection into a single table stream.
        pub mod parallel {
            /// Parallel TCP table reader.
            #[cfg(feature = "tcp")]
            pub mod tcp;

            /// Parallel QUIC table reader.
            #[cfg(feature = "quic")]
            pub mod quic;

            /// Parallel HTTP/2 table reader.
            #[cfg(feature = "http")]
            pub mod http;

            /// Parallel Lightstream protocol reader.
            #[cfg(all(feature = "protocol", feature = "tcp"))]
            pub mod lightstream;
        }

        /// UDS table reader
        #[cfg(feature = "uds")]
        pub mod uds;

        /// Stdin table reader
        #[cfg(feature = "stdio")]
        pub mod stdio;

        /// Lightstream protocol reader
        #[cfg(feature = "protocol")]
        pub mod lightstream;
    }

    /// Writers for Arrow IPC, CSV, and optionally Parquet.
    pub mod writers {
        pub mod ipc {
            /// Sync IPC stream writer.
            pub mod table_stream;

            /// Async IPC file/stream writer.
            pub mod table;

            /// Sync end-to-end IPC writer over `std::io::Write`.
            pub mod sync_table;
        }

        /// CSV writer - for both file and network contexts
        #[cfg(feature = "csv")]
        pub mod csv;

        /// JSON writer - array-of-objects or NDJSON, any io::Write sink
        #[cfg(feature = "json")]
        pub mod json;

        /// Parquet writer
        #[cfg(feature = "parquet")]
        pub mod parquet;

        /// Chunked file writers - write each batch to a separate
        /// `<base>-NNNNN.<ext>` file inside a directory.
        pub mod chunked {
            /// Chunked CSV writer.
            #[cfg(feature = "csv")]
            pub mod csv;

            /// Chunked Parquet writer.
            #[cfg(feature = "parquet")]
            pub mod parquet;

            /// Chunked Arrow IPC writer. Holds a tokio runtime internally
            /// to drive the async IPC file writer.
            pub mod arrow;
        }

        /// TCP table writer
        #[cfg(feature = "tcp")]
        pub mod tcp;

        /// WebSocket table writer
        #[cfg(feature = "websocket")]
        pub mod websocket;

        /// QUIC table writer
        #[cfg(feature = "quic")]
        pub mod quic;

        /// WebTransport table writer
        #[cfg(feature = "webtransport")]
        pub mod webtransport;

        /// HTTP/2 table writer (POST; streaming request body)
        #[cfg(feature = "http")]
        pub mod http;

        /// Parallel transport writers - fan a table sequence across
        /// several concurrent streams on one connection.
        pub mod parallel {
            /// Parallel TCP table writer.
            #[cfg(feature = "tcp")]
            pub mod tcp;

            /// Parallel QUIC table writer.
            #[cfg(feature = "quic")]
            pub mod quic;

            /// Parallel HTTP/2 table writer.
            #[cfg(feature = "http")]
            pub mod http;

            /// Parallel Lightstream protocol writer.
            #[cfg(all(feature = "protocol", feature = "tcp"))]
            pub mod lightstream;
        }

        /// UDS table writer
        #[cfg(feature = "uds")]
        pub mod uds;

        /// Stdout table writer
        #[cfg(feature = "stdio")]
        pub mod stdio;

        /// Lightstream protocol writer
        #[cfg(feature = "protocol")]
        pub mod lightstream;
    }

    /// Stream adapters and sources.
    pub mod streams {
        /// Zero-allocation stream arena for network I/O.
        pub mod stream_arena;

        /// Generic async byte stream adapter for any `AsyncRead` source.
        pub mod async_read;

        /// Async disk-to-buffer stream.
        pub mod disk;

        /// Framed byte stream adapter.
        pub mod framed_byte_stream;

        /// TCP byte stream adapter.
        #[cfg(feature = "tcp")]
        pub mod tcp;

        /// WebSocket byte stream and sink adapters.
        #[cfg(feature = "websocket")]
        pub mod websocket;

        /// QUIC byte stream adapter.
        #[cfg(feature = "quic")]
        pub mod quic;

        /// WebTransport byte stream adapter.
        #[cfg(feature = "webtransport")]
        pub mod webtransport;

        /// HTTP/2 byte stream adapter.
        #[cfg(feature = "http")]
        pub mod http;

        /// UDS byte stream adapter.
        #[cfg(feature = "uds")]
        pub mod uds;

        /// Stdin byte stream adapter.
        #[cfg(feature = "stdio")]
        pub mod stdio;
    }

    /// Codecs for Arrow IPC and Lightstream protocol.
    pub mod codecs;

    /// Per-format implementations of [`crate::traits::serialise::Serialise`]
    /// for the top-level minarrow value types.
    pub mod serialise {
        /// `Serialise<Ipc>` impls for Arrow IPC Stream payloads.
        pub mod ipc;
    }

    /// Protocol modules for Arrow IPC and the Lightstream multiplexer.
    pub mod protocol;

    /// Arrow and Parquet type mappings.
    pub mod types {
        /// Parquet <-> Arrow type bindings.
        #[cfg(feature = "parquet")]
        pub mod parquet;
    }

    /// Custom Memory-map implementation
    #[cfg(feature = "mmap")]
    pub mod mmap;

    /// io_uring-based UDS transport
    #[cfg(feature = "io_uring")]
    pub mod io_uring;
}

/// FlatBuffers-compiled Arrow IPC metadata support.
pub mod arrow {
    /// Flatbuffers Arrow file metadata
    pub mod file;

    /// Flatbuffers Arrow IPC messages
    pub mod message;

    /// Flatbuffers Arrow schema helpers.
    pub mod schema;
}

/// Compression options and helpers.
pub mod compression;

/// Shared protocol constants.
pub mod constants;

/// Internal enums for decode results, protocol kinds, etc.
pub mod enums;

/// Crate-wide error type.
pub mod error;

/// Utility helpers
pub mod utils;

/// Internal test support
#[cfg(test)]
pub(crate) mod test_helpers;

// Re-exports for Arrow FlatBuffers
pub use crate::arrow::message::org::apache::arrow::flatbuf::Message as AFMessage;
pub use crate::arrow::message::org::apache::arrow::flatbuf::MessageHeader as AFMessageHeader;
