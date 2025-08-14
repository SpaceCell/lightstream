//! TLV (Type-Length-Value) Protocol Example
//!
//! This example demonstrates how to:
//! - Create and encode TLV frames
//! - Use TLV streaming writers
//! - Decode TLV frames from binary data
//! - Use TLV sinks for async I/O

use futures_util::SinkExt;
use lightstream::models::encoders::tlv::protocol::TLVEncoder;
use lightstream::models::encoders::tlv::tlv_stream::TLVStreamWriter;
use lightstream::models::frames::tlv_frame::TLVFrame;
use lightstream::models::sinks::tlv_sink::TLVSink;
use lightstream::traits::frame_encoder::FrameEncoder;
use minarrow::Vec64;
use tokio::io::{AsyncReadExt, duplex};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("TLV Protocol Example");
    println!("===================");

    // Example 1: Basic TLV encoding
    println!("\n1. Basic TLV Encoding");
    basic_tlv_encoding()?;

    // Example 2: TLV streaming
    println!("\n2. TLV Streaming");
    tlv_streaming().await?;

    // Example 3: TLV decoding
    println!("\n3. TLV Decoding");
    tlv_decoding().await?;

    // Example 4: Async TLV sink
    println!("\n4. Async TLV Sink");
    async_tlv_sink().await?;

    println!("\n✓ All TLV examples completed successfully!");

    Ok(())
}

/// Demonstrate basic TLV encoding
fn basic_tlv_encoding() -> Result<(), Box<dyn std::error::Error>> {
    println!("  Creating TLV frames...");

    // Create some sample TLV frames
    let frames = vec![
        TLVFrame {
            t: 1,
            value: b"Hello",
        },
        TLVFrame {
            t: 2,
            value: b"World",
        },
        TLVFrame {
            t: 42,
            value: &[0xDE, 0xAD, 0xBE, 0xEF],
        },
        TLVFrame { t: 100, value: b"" }, // Empty value
    ];

    for (i, frame) in frames.iter().enumerate() {
        let mut global_offset = 0;
        let (encoded, _metadata) = TLVEncoder::encode::<Vec64<u8>>(&mut global_offset, frame)?;

        println!(
            "  Frame {}: Type={}, Length={}, EncodedSize={} bytes",
            i + 1,
            frame.t,
            frame.value.len(),
            encoded.len()
        );
        println!("    Hex: {:02X?}", encoded.as_ref());

        // Verify the encoding format
        let expected_size = 1 + 4 + frame.value.len(); // type (1) + length (4) + value
        assert_eq!(encoded.len(), expected_size, "Encoded size mismatch");

        // Check type byte
        assert_eq!(encoded[0], frame.t, "Type byte mismatch");

        // Check length bytes (little-endian)
        let len_bytes = &encoded[1..5];
        let decoded_len =
            u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]);
        assert_eq!(
            decoded_len as usize,
            frame.value.len(),
            "Length encoding mismatch"
        );

        // Check value bytes
        if !frame.value.is_empty() {
            assert_eq!(&encoded[5..], frame.value, "Value bytes mismatch");
        }
    }

    println!("  ✓ All frames encoded correctly");
    Ok(())
}

/// Demonstrate TLV streaming
async fn tlv_streaming() -> Result<(), Box<dyn std::error::Error>> {
    println!("  Creating TLV stream...");

    let mut writer = TLVStreamWriter::<Vec64<u8>>::new();

    // Write several frames
    writer.write_frame(10, b"Stream")?;
    writer.write_frame(20, b"Data")?;
    writer.write_frame(30, &[1, 2, 3, 4, 5])?;

    // Finish the stream
    writer.finish();

    println!("  Reading frames from stream...");
    let mut frame_count = 0;

    use futures_util::stream::Stream;
    use std::pin::Pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    // Helper function to create a dummy waker
    fn dummy_waker() -> Waker {
        fn no_op(_: *const ()) {}
        static VTABLE: RawWakerVTable =
            RawWakerVTable::new(|_| dummy_raw_waker(), no_op, no_op, no_op);
        fn dummy_raw_waker() -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        unsafe { Waker::from_raw(dummy_raw_waker()) }
    }

    let mut pin_writer = Pin::new(&mut writer);
    let waker = dummy_waker();
    let mut cx = Context::from_waker(&waker);

    while let Poll::Ready(Some(result)) = pin_writer.as_mut().poll_next(&mut cx) {
        let frame = result?;
        frame_count += 1;

        // Parse the frame manually to show the structure
        let frame_type = frame[0];
        let len_bytes = &frame[1..5];
        let length = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]);
        let value = &frame[5..];

        println!(
            "  Frame {}: Type={}, Length={}, Value={:02X?}",
            frame_count, frame_type, length, value
        );
    }

    println!("  ✓ Processed {} frames from stream", frame_count);
    Ok(())
}

/// Demonstrate TLV decoding (manual parsing)
async fn tlv_decoding() -> Result<(), Box<dyn std::error::Error>> {
    println!("  Creating binary TLV data...");

    // Manually create some TLV binary data
    let mut data = Vec::new();

    // Frame 1: Type 5, Value "Test"
    data.push(5u8);
    data.extend_from_slice(&(4u32.to_le_bytes())); // length = 4
    data.extend_from_slice(b"Test");

    // Frame 2: Type 99, Value [0xFF, 0x00, 0xFF]
    data.push(99u8);
    data.extend_from_slice(&(3u32.to_le_bytes())); // length = 3
    data.extend_from_slice(&[0xFF, 0x00, 0xFF]);

    println!("  Binary data ({} bytes): {:02X?}", data.len(), data);

    // Manual parsing (since TLVDecoder import has issues in examples)
    println!("  Manually parsing frames...");
    let mut offset = 0;
    let mut decoded_count = 0;

    while offset < data.len() {
        if offset + 5 > data.len() {
            break;
        }

        let frame_type = data[offset];
        let len_bytes = &data[offset + 1..offset + 5];
        let length =
            u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;

        if offset + 5 + length > data.len() {
            break;
        }

        let value = &data[offset + 5..offset + 5 + length];
        decoded_count += 1;

        println!(
            "  Decoded frame {}: Type={}, Length={}, Value={:02X?}",
            decoded_count, frame_type, length, value
        );

        offset += 5 + length;
    }

    println!("  ✓ Decoded {} frames manually", decoded_count);
    Ok(())
}

/// Demonstrate async TLV sink
async fn async_tlv_sink() -> Result<(), Box<dyn std::error::Error>> {
    println!("  Creating async TLV sink...");

    let (client, mut server) = duplex(256);
    let mut sink = TLVSink::<_, Vec64<u8>>::new(client);

    // Send frames using the async Sink interface
    let frames = vec![
        TLVFrame {
            t: 200,
            value: b"Async",
        },
        TLVFrame {
            t: 201,
            value: b"TLV",
        },
        TLVFrame {
            t: 202,
            value: b"Sink",
        },
    ];

    println!("  Sending {} frames...", frames.len());
    for (i, frame) in frames.iter().enumerate() {
        sink.send(TLVFrame {
            t: frame.t,
            value: frame.value,
        })
        .await?;
        println!(
            "  Sent frame {}: Type={}, Value={:?}",
            i + 1,
            frame.t,
            String::from_utf8_lossy(frame.value)
        );
    }

    // Close the sink (flushes remaining data)
    sink.close().await?;
    println!("  Sink closed and flushed");

    // Read and verify the data on the server side
    println!("  Reading data from server side...");
    let mut total_bytes = 0;

    for (i, expected_frame) in frames.iter().enumerate() {
        let frame_size = 1 + 4 + expected_frame.value.len();
        let mut buffer = vec![0u8; frame_size];
        server.read_exact(&mut buffer).await?;
        total_bytes += buffer.len();

        // Parse the received frame
        let frame_type = buffer[0];
        let len_bytes = &buffer[1..5];
        let length = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]);
        let value = &buffer[5..];

        println!(
            "  Received frame {}: Type={}, Length={}, Value={:?}",
            i + 1,
            frame_type,
            length,
            String::from_utf8_lossy(value)
        );

        // Verify the frame matches what we sent
        assert_eq!(frame_type, expected_frame.t, "Frame type mismatch");
        assert_eq!(
            length as usize,
            expected_frame.value.len(),
            "Frame length mismatch"
        );
        assert_eq!(value, expected_frame.value, "Frame value mismatch");
    }

    // Verify no more data
    let mut end_buffer = [0u8; 1];
    let bytes_read = server.read(&mut end_buffer).await?;
    assert_eq!(bytes_read, 0, "Expected end of stream");

    println!(
        "  ✓ Received and verified {} bytes across {} frames",
        total_bytes,
        frames.len()
    );
    Ok(())
}
