//! # Lightstream - Streaming Arrow IPC, TLV, and Parquet I/O and Transport for Extreme Performance Data
//!
//! **Lightstream** includes composable building blocks for high-performance data I/O in Rust.
//!
//! It extends [Minarrow](https://crates.io/crates/minarrow) with a set of modular, format-aware components for:
//!
//! - High-performance asynchronous Arrow IPC streaming and file writing
//! - Framed decoders and sinks for `IPC`, `TLV`, `CSV`, and opt-in `Parquet`
//! - Zero-Copy memory-mapped Arrow file reads
//! - Direct Tokio integration with zero-copy buffers
//! - 64-byte SIMD aligned readers and writers - *(not generally available in the Arrow ecosystem)*
//!
//! Then, on top of that, it includes the high performance Lightstream protocol and out of the box support for
//!     - Arrow IPC (including mmap, File and Stream protocols)
//!     - CSV
//!     - Parquet
//!     - QUIC
//!     - IOUring for kernel bypass    
//!     - Stdin/StdOut
//!     - TCP
//!     - UDS
//!     - Websocket
//!     - Webtransport
//!     - Memory Mapped IPC Readers/Writers + Parquet
//!     
//! These are built via a consistent interface where the protocol is agnostic to the transport, making adding new ones trivial.
//!
//! Additionally, it is highly customisable as the layered architecture allows opting up to the desired level for modifying and/or implementing
//! new protocols as needed.
//!
//! ## Highlights
//!
//! - ✅ Fully async-compatible with [`tokio::io::AsyncWrite`]  
//! - ✅ Pluggable encoders and frame formats  
//! - ✅ Arrow IPC framing with dictionary + schema support  
//! - ✅ Categorical dictionary support
//! - ✅ Compatible with [`minarrow::Table`] and [`minarrow::SuperTable`]  
//! - ✅ Feature flags for `parquet`, `zstd`, `snappy`, compression and `mmap`  
//!
//! ## Example - Arrow Table Writer
//!
//! ```rust,no_run
//! use minarrow::{arr_i32, arr_str32, vec64, FieldArray, Table};
//! use lightstream::models::writers::ipc::table::TableWriter;
//! use lightstream::enums::IPCMessageProtocol;
//! use tokio::fs::File;
//!
//! # async fn write() -> std::io::Result<()> {
//! let col1 = FieldArray::from_arr("ids", arr_i32![1, 2, 3]);
//! let col2 = FieldArray::from_arr("names", arr_str32!["a", "b", "c"]);
//! let table = Table::new("example".to_string(), vec![col1, col2].into());
//!
//! let file = File::create("out.arrow").await?;
//!
//! let mut writer = TableWriter::from_schema(file, table.schema(), IPCMessageProtocol::File)?;
//! writer.write_table(table).await?;
//! writer.finish().await?;
//! # Ok(()) }
//! ```
//!
//! See the [README](https://github.com/pbower/lightstream) for more examples.

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
            /// Parallel QUIC table reader.
            #[cfg(feature = "quic")]
            pub mod quic;

            /// Parallel HTTP/2 table reader.
            #[cfg(feature = "http")]
            pub mod http;
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
            /// Parallel QUIC table writer.
            #[cfg(feature = "quic")]
            pub mod quic;

            /// Parallel HTTP/2 table writer.
            #[cfg(feature = "http")]
            pub mod http;
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
