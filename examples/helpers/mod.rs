use minarrow::{arr_f64, arr_i32, arr_str32, FieldArray, Table, Vec64};

#[cfg(feature = "msgpack")]
use futures_core::Stream;
#[cfg(feature = "msgpack")]
use lightstream::models::protocol::connection::LightstreamConnection;
#[cfg(feature = "msgpack")]
use lightstream::models::protocol::LightstreamMessage;
#[cfg(feature = "msgpack")]
use tokio::io::AsyncWrite;

/// Shared command type sent as msgpack in all examples.
#[cfg(feature = "msgpack")]
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Command {
    pub action: String,
    pub timestamp_ms: u64,
    pub params: Vec<String>,
}

/// Build a small table with int, float, and string columns.
pub fn make_table(label: &str, n_rows: usize) -> Table {
    let ids: Vec64<i32> = (0..n_rows as i32).collect();
    let values: Vec64<f64> = (0..n_rows).map(|i| i as f64 * 0.5).collect();
    let labels: Vec64<String> = (0..n_rows).map(|i| format!("{}_{}", label, i)).collect();
    let label_refs: Vec64<&str> = labels.iter().map(String::as_str).collect();

    let id_col = FieldArray::from_arr("id", arr_i32!(ids));
    let value_col = FieldArray::from_arr("value", arr_f64!(values));
    let label_col = FieldArray::from_arr("label", arr_str32!(label_refs));

    Table::new(label.to_string(), Some(vec![id_col, value_col, label_col]))
}

/// Extract a cloned schema from a table, suitable for type registration.
pub fn table_schema() -> Vec<minarrow::Field> {
    let sample = make_table("_schema", 0);
    sample.schema().iter().map(|f| (**f).clone()).collect()
}

// ---------------------------------------------------------------------------
// Shared protocol demo helpers for transport examples
// ---------------------------------------------------------------------------

/// Register the three demo channel types used by all transport examples:
/// - "raw" i.e. tag 0, opaque byte payloads
/// - "command" i.e. tag 1, msgpack-encoded `Command` structs
/// - "metrics" i.e. Arrow table channel
#[cfg(feature = "msgpack")]
pub fn register_demo_types<S, W>(conn: &mut LightstreamConnection<S, W>)
where
    S: Stream<Item = Result<Vec<u8>, std::io::Error>> + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    conn.register_message("raw");
    conn.register_message("command");
    conn.register_table("metrics", table_schema());
}

/// Send a representative mix of raw, msgpack, and Arrow table messages,
/// then flush and shut down the connection.
#[cfg(feature = "msgpack")]
pub async fn send_demo_messages<S, W>(
    conn: &mut LightstreamConnection<S, W>,
    label: &str,
) -> std::io::Result<()>
where
    S: Stream<Item = Result<Vec<u8>, std::io::Error>> + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    conn.send("raw", format!("hello-{}", label).as_bytes())
        .await?;
    conn.send_msgpack(
        "command",
        &Command {
            action: "ingest".into(),
            timestamp_ms: 1_700_000_000_000,
            params: vec![format!("--source={}", label)],
        },
    )
    .await?;
    conn.send_table("metrics", &make_table(label, 4)).await?;
    conn.send("raw", format!("goodbye-{}", label).as_bytes())
        .await?;
    conn.flush().await?;
    conn.shutdown().await?;
    Ok(())
}

/// Receive and print all messages until the connection closes.
#[cfg(feature = "msgpack")]
pub async fn recv_and_print_all<S, W>(conn: &mut LightstreamConnection<S, W>)
where
    S: Stream<Item = Result<Vec<u8>, std::io::Error>> + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    while let Some(result) = conn.recv().await {
        let msg = result.unwrap();
        match &msg {
            LightstreamMessage::Message { tag, payload } => {
                if *tag == 0 {
                    println!(
                        "  [raw] {:?}",
                        std::str::from_utf8(payload).unwrap()
                    );
                } else {
                    let cmd: Command = msg.decode_msgpack().unwrap();
                    println!("  [command] {:?}", cmd);
                }
            }
            LightstreamMessage::Table { table, .. } => {
                println!(
                    "  [table] {} rows, {} cols",
                    table.n_rows,
                    table.cols.len()
                );
            }
        }
    }
}
