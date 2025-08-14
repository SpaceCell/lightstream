//! # Lightstream — Streaming Arrow IPC, TLV, and Parquet I/O for Minarrow
//!
//! **Lightstream** provides composable building blocks for high-performance data I/O in Rust.
//!
//! It extends [Minarrow](https://crates.io/crates/minarrow) with a set of modular, format-aware components for:
//!
//! - High-performance asynchronous Arrow IPC streaming and file writing
//! - Framed decoders and sinks for `IPC`, `TLV`, `CSV`, and opt-in `Parquet`
//! - Zero-Copy memory-mapped Arrow file reads
//! - Direct Tokio integration with zero-copy buffers
//! - 64-byte SIMD aligned readers and writers *(the only Arrow crate that provides this in 2025)*
//!
//! ## Design Principles
//!
//! - **Customisable** - ***You own the buffer*** – No hidden buffering or lifecycle surprises. All streaming is pull-based or sink-driven.
//! - **Composable** - ***Layerable codecs*** – Each encoder, decoder, sink, and stream adapter is layerable, and your bytestream propagates up.
//! - **Control** - ***Wire-level framing*** – Arrow IPC, TLV, CSV, and Parquet handled at the transport boundary, not fused into business logic.
//! - **Power** - **64-byte aligned by default** – All buffers use 64-byte aligned memory via [`Vec64`] for deterministic SIMD - not re-allocating
//! during hotloop calculations where you need it fast.
//! - **Extensible** - all primitives are provided to create your own data wire formats, and customise it to your stack. We also welcome contributions.
//! 
//! ## Highlights
//!
//! - ✅ Fully async-compatible with [`tokio::io::AsyncWrite`]  
//! - ✅ Pluggable encoders and frame formats  
//! - ✅ Arrow IPC framing with dictionary + schema support  
//! - ✅ Categorical dictionary support
//! - ✅ Compatible with [`minarrow::Table`] and [`minarrow::SuperTable`]  
//! - ✅ Optional support for `parquet`, `zstd`, `snappy`, and `mmap`  
//!
//! ## Example — Arrow Table Writer
//!
//! ***rust
//! use minarrow::{arr_i32, arr_str32, FieldArray, Table};
//! use lightstream::models::writers::ipc::table_writer::TableWriter;
//! use lightstream::enums::IPCMessageProtocol;
//! use tokio::fs::File;
//!
//! # async fn write() -> std::io::Result<()> {
//! let col1 = FieldArray::from_inner("ids", arr_i32![1, 2, 3]);
//! let col2 = FieldArray::from_inner("names", arr_str32!["a", "b", "c"]);
//! let table = Table::new("example", vec![col1, col2].into());
//!
//! let schema = table.schema().to_vec();
//! let file = File::create("out.arrow").await?;
//!
//! let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::File)?;
//! writer.write_table(table).await?;
//! writer.finish().await?;
//! # Ok(()) }
//! ***
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
}

/// Codec implementations, readers, writers, and I/O models
pub mod models {
    /// Sinks convert tables or TLV frames into byte streams
    pub mod sinks {
        /// Arrow IPC sink - Stream/File protocols
        pub mod table_sink;

        /// TLV sink for simple type-length-value framing
        pub mod tlv_sink;
    }

    /// Encoders for Arrow IPC, TLV, CSV, and optionally Parquet
    pub mod encoders {
        /// Arrow IPC encoders
        pub mod ipc {
            /// IPC protocol framing
            pub mod protocol;

            /// IPC schema serialisation.
            pub mod schema;

            /// Stream encoder for Arrow tables
            pub mod table_stream;
        }

        /// Parquet encoders (if `parquet` feature is enabled)
        #[cfg(feature = "parquet")]
        pub mod parquet {
            /// Low-level value encoders
            pub mod data;

            /// Page and metadata writers
            pub mod metadata;
        }

        /// TLV wire format encoders
        pub mod tlv {
            /// TLV framing protocol
            pub mod protocol;

            /// Buffered TLV stream writer
            pub mod tlv_stream;
        }

        /// CSV encoder for tables and supertables
        pub mod csv;
    }

    /// Decoders for Arrow IPC, CSV, TLV, and optionally Parquet
    pub mod decoders {
        /// Arrow IPC decoders
        pub mod ipc {
            /// FlatBuffer parser for record batches
            pub mod parser;

            /// IPC protocol parser
            pub mod protocol;

            /// Streaming decoder for Arrow tables
            pub mod table_stream;
        }

        /// CSV-to-table decoder
        pub mod csv;

        /// Parquet decoder (if `parquet` feature is enabled)
        #[cfg(feature = "parquet")]
        pub mod parquet;

        /// TLV stream decoder
        pub mod tlv;
    }

    /// Frame structures for IPC and TLV
    pub mod frames {
        /// IPC message wrappers
        pub mod ipc_message;

        /// TLV frame definitions
        pub mod tlv_frame;
    }

    /// Readers for files, mmap, and async streams
    pub mod readers {
        /// Arrow IPC readers
        pub mod ipc {
            /// File-based IPC reader
            pub mod file_table_reader;

            /// 64-Byte Aligned Zero Copy Mmap IPC reader
            #[cfg(feature = "mmap")]
            pub mod mmap_table_reader;

            /// Streamed IPC table reader
            pub mod table_reader;

            /// Stream adapter yielding Arrow tables
            pub mod table_stream_reader;
        }

        /// CSV reader utilities.
        pub mod csv_reader;

        /// Parquet reader
        #[cfg(feature = "parquet")]
        pub mod parquet_reader;
    }

    /// Writers for Arrow IPC, CSV, and optionally Parquet.
    pub mod writers {
        pub mod ipc {
            /// Sync IPC stream writer.
            pub mod table_stream_writer;

            /// Async IPC file/stream writer.
            pub mod table_writer;
        }

        /// CSV writer - for both file and network contexts
        pub mod csv_writer;

        /// Parquet writer
        #[cfg(feature = "parquet")]
        pub mod parquet_writer;
    }

    /// Stream adapters and sources.
    pub mod streams {
        /// Async disk-to-buffer stream.
        pub mod disk;

        /// Framed byte stream adapter.
        pub mod framed_byte_stream;
    }

    /// Arrow and Parquet type mappings.
    pub mod types {
        /// Parquet <-> Arrow type bindings.
        #[cfg(feature = "parquet")]
        pub mod parquet;
    }

    /// Custom Memory-map implementation
    #[cfg(feature = "mmap")]
    pub mod mmap;
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
