//! Lightstream Protocol Example
//!
//! This is the core tutorial for the Lightstream protocol layer. It uses TCP
//! as the transport since it is the simplest, but the focus is on the protocol
//! concepts:
//!
//! 1. Type registration: raw messages, msgpack messages, and Arrow tables
//! 2. Raw messages: send and receive opaque `&[u8]` payloads
//! 3. Msgpack messages: send typed structs, decode on receive
//! 4. Arrow tables: send tables, verify schema persistence across batches
//! 5. Mixed stream: interleave all three on one connection
//!
//! Run with:
//! ```sh
//! cargo run --example lightstream --features "protocol,tcp,msgpack"
//! ```

mod helpers;

use helpers::{Command, make_table, table_schema};
use lightstream::models::protocol::LightstreamMessage;
use lightstream::models::protocol::connection::TcpLightstreamConnection;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightstream Protocol Example");
    println!("============================\n");

    // --- 1. Raw messages -------------------------------------------------
    println!("1. Raw Messages");
    raw_messages().await?;

    // --- 2. Msgpack messages ---------------------------------------------
    println!("\n2. Msgpack Messages");
    msgpack_messages().await?;

    // --- 3. Arrow tables -------------------------------------------------
    println!("\n3. Arrow Tables");
    arrow_tables().await?;

    // --- 4. Mixed stream -------------------------------------------------
    println!("\n4. Mixed Stream (raw + msgpack + tables interleaved)");
    mixed_stream().await?;

    println!("\nAll Lightstream protocol examples completed successfully!");
    Ok(())
}

// --------------------------------------------------------------------------
// 1. Raw messages
// --------------------------------------------------------------------------
async fn raw_messages() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut conn = TcpLightstreamConnection::from_tcp(stream);
        conn.register_message("ping");
        conn.register_message("pong");

        // Receive a ping
        let msg = conn.recv().await.unwrap().unwrap();
        assert!(msg.is_message());
        let payload = msg.payload().unwrap();
        println!(
            "  Server received: {:?}",
            std::str::from_utf8(payload).unwrap()
        );

        // Send a pong
        conn.send("pong", b"pong-reply").await.unwrap();
        conn.flush().await.unwrap();
        conn.shutdown().await.unwrap();
    });

    let client_stream = tokio::net::TcpStream::connect(addr).await?;
    let mut client = TcpLightstreamConnection::from_tcp(client_stream);
    client.register_message("ping");
    client.register_message("pong");

    // Send a ping
    client.send("ping", b"hello-server").await?;
    client.flush().await?;

    // Receive a pong
    let msg = client.recv().await.unwrap()?;
    assert!(msg.is_message());
    let payload = msg.payload().unwrap();
    println!(
        "  Client received: {:?}",
        std::str::from_utf8(payload).unwrap()
    );

    client.shutdown().await?;
    server.await?;
    println!("  Raw message round-trip complete.");
    Ok(())
}

// --------------------------------------------------------------------------
// 2. Msgpack messages
// --------------------------------------------------------------------------
async fn msgpack_messages() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut conn = TcpLightstreamConnection::from_tcp(stream);
        conn.register_message("command");

        // Receive two commands
        for _ in 0..2 {
            let msg = conn.recv().await.unwrap().unwrap();
            let cmd: Command = msg.decode_msgpack().unwrap();
            println!("  Server decoded: {:?}", cmd);
        }
    });

    let client_stream = tokio::net::TcpStream::connect(addr).await?;
    let mut client = TcpLightstreamConnection::from_tcp(client_stream);
    client.register_message("command");

    // Send typed msgpack commands
    let cmd1 = Command {
        action: "start".into(),
        timestamp_ms: 1_700_000_000_000,
        params: vec!["--verbose".into()],
    };
    let cmd2 = Command {
        action: "stop".into(),
        timestamp_ms: 1_700_000_001_000,
        params: vec![],
    };

    client.send_msgpack("command", &cmd1).await?;
    client.send_msgpack("command", &cmd2).await?;
    client.flush().await?;
    client.shutdown().await?;

    server.await?;
    println!("  Msgpack round-trip complete.");
    Ok(())
}

// --------------------------------------------------------------------------
// 3. Arrow tables
// --------------------------------------------------------------------------
async fn arrow_tables() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut conn = TcpLightstreamConnection::from_tcp(stream);
        conn.register_table("metrics", table_schema());

        // Receive two table batches
        for i in 0..2 {
            let msg = conn.recv().await.unwrap().unwrap();
            let table = msg.table().unwrap();
            println!(
                "  Server received table batch {}: {} rows, {} cols",
                i + 1,
                table.n_rows,
                table.cols.len()
            );
            // Verify schema persists across batches
            assert_eq!(table.cols[0].field.name, "id");
            assert_eq!(table.cols[1].field.name, "value");
            assert_eq!(table.cols[2].field.name, "label");
        }
    });

    let client_stream = tokio::net::TcpStream::connect(addr).await?;
    let mut client = TcpLightstreamConnection::from_tcp(client_stream);
    client.register_table("metrics", table_schema());

    // Send two batches. The first carries the schema header; the second
    // sends only the record batch, because schema state persists.
    client
        .send_table("metrics", &make_table("batch1", 5))
        .await?;
    client
        .send_table("metrics", &make_table("batch2", 3))
        .await?;
    client.flush().await?;
    client.shutdown().await?;

    server.await?;
    println!("  Arrow table round-trip complete (schema persisted across batches).");
    Ok(())
}

// --------------------------------------------------------------------------
// 4. Mixed stream
// --------------------------------------------------------------------------
async fn mixed_stream() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut conn = TcpLightstreamConnection::from_tcp(stream);
        conn.register_message("raw");
        conn.register_message("command");
        conn.register_table("metrics", table_schema());

        let mut raw_count = 0u32;
        let mut msgpack_count = 0u32;
        let mut table_count = 0u32;

        while let Some(result) = conn.recv().await {
            let msg = result.unwrap();
            match msg {
                LightstreamMessage::Table { table, .. } => {
                    table_count += 1;
                    println!("  Server got table: {} rows", table.n_rows);
                }
                LightstreamMessage::Message { tag, ref payload } => {
                    // tag 0 = "raw", tag 1 = "command"
                    if tag == 0 {
                        raw_count += 1;
                        println!(
                            "  Server got raw: {:?}",
                            std::str::from_utf8(payload).unwrap()
                        );
                    } else {
                        msgpack_count += 1;
                        let cmd: Command = msg.decode_msgpack().unwrap();
                        println!("  Server got command: {:?}", cmd);
                    }
                }
            }
        }

        println!(
            "  Server totals: {} raw, {} msgpack, {} table",
            raw_count, msgpack_count, table_count
        );
    });

    let client_stream = tokio::net::TcpStream::connect(addr).await?;
    let mut client = TcpLightstreamConnection::from_tcp(client_stream);
    client.register_message("raw");
    client.register_message("command");
    client.register_table("metrics", table_schema());

    // Interleave different message types on one connection
    client.send("raw", b"first-raw-message").await?;

    client
        .send_msgpack(
            "command",
            &Command {
                action: "deploy".into(),
                timestamp_ms: 1_700_000_002_000,
                params: vec!["--region=us-east".into()],
            },
        )
        .await?;

    client
        .send_table("metrics", &make_table("mixed", 4))
        .await?;

    client.send("raw", b"second-raw-message").await?;

    client
        .send_msgpack(
            "command",
            &Command {
                action: "rollback".into(),
                timestamp_ms: 1_700_000_003_000,
                params: vec![],
            },
        )
        .await?;

    client
        .send_table("metrics", &make_table("mixed2", 2))
        .await?;

    client.flush().await?;
    client.shutdown().await?;

    server.await?;
    println!("  Mixed stream complete.");
    Ok(())
}
