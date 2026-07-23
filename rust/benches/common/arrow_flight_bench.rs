// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Arrow Flight benchmark scaffolding shared by the loopback comparison bench
//! and the cross-host benchmark.
//!
//! Verifies the zero-copy Arrow export of a bench table and serves it over a
//! minimal Flight service whose `DoGet` returns the pre-built batch repeated
//! the number of times the ticket requests. Every other RPC is unsupported.
//!
//! The 8 MiB HTTP/2 windows remove the transport-level flow-control
//! ceiling. Flight-data slicing stays at the encoder's default 2 MiB.
//! The encoder resends dictionaries, which ensures Flight uses the more
//! efficient dictionary-encoded representation rather than defaulting to
//! actual strings for compatibility reasons, so the wire carries the same
//! representation Lightstream sends.

#![allow(dead_code)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    encode::{DictionaryHandling, FlightDataEncoderBuilder},
};
use futures::stream::{self, BoxStream, TryStreamExt};
use minarrow::ffi::arrow_dtype::CategoricalIndexType;
use minarrow::{ArrowType, Table};
use tonic::{Request, Response, Status, Streaming};

/// HTTP/2 flow-control window advertised on both the Flight server and client.
pub const FLIGHT_HTTP2_WINDOW: u32 = 8 * 1024 * 1024;

/// Asserts the zero-copy Arrow export of `table` matches it column for
/// column, with equal row counts, names, and Arrow types of the same bit
/// width, so any type drift in the export fails before measurement.
pub fn assert_export_parity(table: &Table, batch: &RecordBatch) {
    assert_eq!(
        batch.num_rows(),
        table.n_rows,
        "row count drift in the Arrow export"
    );
    assert_eq!(
        batch.num_columns(),
        table.n_cols(),
        "column count drift in the Arrow export"
    );
    for (field, arrow_field) in table.schema().iter().zip(batch.schema().fields()) {
        assert_eq!(
            arrow_field.name(),
            &field.name,
            "column name drift in the Arrow export"
        );
        let expected = match field.dtype {
            ArrowType::Int32 => DataType::Int32,
            ArrowType::Int64 => DataType::Int64,
            ArrowType::Float32 => DataType::Float32,
            ArrowType::Float64 => DataType::Float64,
            ArrowType::String => DataType::Utf8,
            #[cfg(not(feature = "default_categorical_8"))]
            ArrowType::Dictionary(CategoricalIndexType::UInt32) => {
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8))
            }
            #[cfg(feature = "default_categorical_8")]
            ArrowType::Dictionary(CategoricalIndexType::UInt8) => {
                DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8))
            }
            ref other => panic!("bench shapes do not cover the {:?} column type", other),
        };
        assert_eq!(
            arrow_field.data_type(),
            &expected,
            "type drift in the Arrow export for column {}",
            field.name
        );
    }
}


/// Minimal Flight service for the benchmark workloads. `GetFlightInfo`
/// describes an ordered dataset split across the requested number of
/// endpoints.
///
/// `DoGet` serves two callers. The loopback benchmark sends a plain repeat
/// count in its ticket and receives the pre-built batch that many times.
/// The ECS rig's endpoint tickets also carry a partition index, so each
/// endpoint receives its contiguous range of sequenced batches, which the
/// sink verifies for ordered, complete delivery.
#[derive(Clone)]
pub struct BenchFlightService {
    pub batch: Arc<RecordBatch>,
}

#[tonic::async_trait]
impl FlightService for BenchFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights not implemented"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let bytes: [u8; 17] = descriptor
            .cmd
            .as_ref()
            .try_into()
            .map_err(|_| Status::invalid_argument("flight descriptor must be 17 bytes"))?;
        let batches_per_endpoint = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let endpoint_count = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        if endpoint_count == 0 {
            return Err(Status::invalid_argument(
                "flight descriptor must request at least one endpoint",
            ));
        }

        let endpoints = (0..endpoint_count)
            .map(|idx| {
                let mut ticket = Vec::with_capacity(17);
                ticket.extend_from_slice(&batches_per_endpoint.to_le_bytes());
                ticket.extend_from_slice(&idx.to_le_bytes());
                ticket.push(bytes[16]);
                FlightEndpoint::new().with_ticket(Ticket::new(ticket))
            })
            .collect();
        let total_records = batches_per_endpoint
            .checked_mul(endpoint_count)
            .and_then(|n| n.checked_mul(self.batch.num_rows() as u64))
            .and_then(|n| i64::try_from(n).ok())
            .ok_or_else(|| Status::invalid_argument("flight record count exceeds i64"))?;
        let info = FlightInfo::new()
            .try_with_schema(self.batch.schema().as_ref())
            .map_err(|e| Status::internal(format!("encode flight schema: {e}")))?
            .with_descriptor(descriptor)
            .with_endpoints(endpoints)
            .with_total_records(total_records)
            .with_ordered(true);
        Ok(Response::new(info))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not implemented"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema not implemented"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let (n, start_seq) = match ticket.ticket.len() {
            // The loopback benchmark calls DoGet directly and receives the
            // pre-built batch repeated `n` times.
            8 => (u64::from_le_bytes(ticket.ticket.as_ref().try_into().unwrap()), None),
            // ECS endpoint tickets additionally identify the logical
            // partition, which anchors the endpoint's sequence range.
            17 => {
                let n = u64::from_le_bytes(ticket.ticket[0..8].try_into().unwrap());
                let idx = u64::from_le_bytes(ticket.ticket[8..16].try_into().unwrap());
                (n, Some(idx * n))
            }
            _ => return Err(Status::invalid_argument("flight ticket must be 8 or 17 bytes")),
        };
        let batch = Arc::clone(&self.batch);
        let batch_stream = stream::iter((0..n).map(move |b| match start_seq {
            None => Ok((*batch).clone()),
            // Batch `seq` regenerates the leading i32 column to hold
            // `seq + i` at row `i`, which mirrors `replay_batch_table` on
            // the Lightstream side. The remaining columns stay shared with
            // the base export, and the column builds as the stream is
            // polled, so the work lands inside the timed transfer on both
            // transports.
            Some(start) => {
                let seq = start + b;
                let rows = batch.num_rows();
                let first = Int32Array::from_iter_values(
                    (0..rows).map(|i| (seq as i32).wrapping_add(i as i32)),
                );
                let mut cols = batch.columns().to_vec();
                cols[0] = Arc::new(first) as ArrayRef;
                RecordBatch::try_new(batch.schema(), cols).map_err(FlightError::from)
            }
        }));
        // The encoder keeps its default flight-data size, so batches above
        // 2 MiB split into multiple messages per Arrow Flight's own tuning.
        // Resending dictionaries ensures Flight uses the more efficient
        // dictionary-encoded representation rather than defaulting to
        // actual strings for compatibility reasons. Per the upstream
        // documentation at
        // https://docs.rs/arrow-flight/latest/arrow_flight/encode/enum.DictionaryHandling.html
        //
        // "Variants
        //
        //  Hydrate
        //  Expands to the underlying type (default). This likely sends more
        //  data over the network but requires less memory (dictionaries are
        //  not tracked) and is more compatible with other arrow flight
        //  client implementations that may not support DictionaryEncoding
        //
        //  See also:
        //  https://github.com/apache/arrow-rs/issues/1206
        //
        //  Resend
        //  Send dictionary FlightData with every RecordBatch that contains
        //  a DictionaryArray. See Self::Hydrate for more tradeoffs. No
        //  attempt is made to skip sending the same (logical) dictionary
        //  values twice.
        //
        //  This requires identifying the different dictionaries in use and
        //  assigning them unique IDs"
        let builder = FlightDataEncoderBuilder::new()
            .with_dictionary_handling(DictionaryHandling::Resend);
        let flight_data = builder
            .build(batch_stream)
            .map_err(|err| Status::internal(format!("flight encode failure: {err}")));
        Ok(Response::new(Box::pin(flight_data)))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions not implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not implemented"))
    }
}
