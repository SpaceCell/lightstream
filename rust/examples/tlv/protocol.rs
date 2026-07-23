// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TLV (Type-Length-Value) Protocol Example
//!
//! Demonstrates TLV frame encoding, streaming, and async sink I/O.

use futures_util::SinkExt;
use futures_util::stream::StreamExt;
use lightstream::models::encoders::tlv::TLVEncoder;
use lightstream::models::encoders::tlv::tlv_stream::TLVStreamWriter;
use lightstream::models::frames::tlv_frame::TLVFrame;
use lightstream::models::sinks::tlv_sink::TLVSink;
use lightstream::traits::frame_encoder::FrameEncoder;
use minarrow::Vec64;
use tokio::io::{AsyncReadExt, duplex};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Basic encoding
    println!("1. Basic TLV Encoding");
    let frames = [
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
        TLVFrame { t: 100, value: b"" },
    ];
    for frame in &frames {
        let mut offset = 0;
        let (encoded, _) = TLVEncoder::encode::<Vec64<u8>>(&mut offset, frame)?;
        assert_eq!(encoded[0], frame.t);
        assert_eq!(encoded.len(), 1 + 4 + frame.value.len());
        println!(
            "  Type={}, Len={}, Encoded={} bytes",
            frame.t,
            frame.value.len(),
            encoded.len()
        );
    }

    // 2. Stream writer
    println!("\n2. TLV Streaming");
    let mut writer = TLVStreamWriter::<Vec64<u8>>::new();
    writer.write_frame(10, b"Stream")?;
    writer.write_frame(20, b"Data")?;
    writer.write_frame(30, &[1, 2, 3, 4, 5])?;
    writer.finish();

    let mut count = 0;
    while let Some(result) = writer.next().await {
        let frame = result?;
        count += 1;
        let frame_type = frame[0];
        let length = u32::from_le_bytes(frame[1..5].try_into().unwrap());
        println!("  Frame {}: Type={}, Length={}", count, frame_type, length);
    }

    // 3. Async TLV sink over a duplex channel
    println!("\n3. Async TLV Sink");
    let (client, mut server) = duplex(256);
    let mut sink = TLVSink::<_, Vec64<u8>>::new(client);

    let payloads: &[(&[u8], u8)] = &[(b"Async", 200), (b"TLV", 201), (b"Sink", 202)];
    for &(value, t) in payloads {
        sink.send(TLVFrame { t, value }).await?;
    }
    sink.close().await?;

    for &(expected_value, expected_type) in payloads {
        let mut buf = vec![0u8; 1 + 4 + expected_value.len()];
        server.read_exact(&mut buf).await?;
        assert_eq!(buf[0], expected_type);
        assert_eq!(&buf[5..], expected_value);
        println!(
            "  Received: Type={}, Value={:?}",
            buf[0],
            String::from_utf8_lossy(&buf[5..])
        );
    }

    let n = server.read(&mut [0u8; 1]).await?;
    assert_eq!(n, 0, "Stream should be closed");

    println!("\nAll TLV examples completed.");
    Ok(())
}
