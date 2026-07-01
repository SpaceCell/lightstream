// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Arrow Flight benchmark scaffolding shared by the loopback comparison bench
//! and the cross-host benchmark.
//!
//! Builds an Arrow `RecordBatch` for each [`BenchShape`] and serves it over a
//! minimal Flight service whose `DoGet` returns the pre-built batch repeated
//! the number of times the ticket requests. Every other RPC is unsupported.
//!
//! The 8 MiB HTTP/2 windows and the raised message and flight-data limits let
//! each batch travel as one message rather than the default 2 MiB slices.

#![allow(dead_code)]

use std::sync::Arc;

use arrow::array::{
    ArrayRef, DictionaryArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field as ArrowField, Int32Type, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    encode::FlightDataEncoderBuilder,
};
use futures::stream::{self, BoxStream, TryStreamExt};
use tonic::{Request, Response, Status, Streaming};

use super::bench_helpers::BenchShape;

const STRING_HEAVY_DICT_CARDINALITY: usize = 100;
const WIDE_GROUP_SIZE: usize = 25;

/// HTTP/2 flow-control window advertised on both the Flight server and client.
pub const FLIGHT_HTTP2_WINDOW: u32 = 8 * 1024 * 1024;

/// gRPC and flight-data size limit, raised so each batch ships as one message.
pub const FLIGHT_MAX_MESSAGE_BYTES: usize = i32::MAX as usize;

/// Build the Arrow record batch matching `shape` at `n_rows` rows.
pub fn make_record_batch(shape: BenchShape, n_rows: usize) -> RecordBatch {
    match shape {
        BenchShape::Mixed => mixed_batch(n_rows),
        BenchShape::NarrowNumeric => narrow_numeric_batch(n_rows),
        BenchShape::StringHeavy => string_heavy_batch(n_rows),
        BenchShape::Wide => wide_batch(n_rows),
    }
}

fn mixed_batch(n_rows: usize) -> RecordBatch {
    let ids = Int32Array::from((0..n_rows as i32).collect::<Vec<_>>());
    let values = Float64Array::from((0..n_rows).map(|i| i as f64 * 0.5).collect::<Vec<_>>());
    let labels =
        StringArray::from((0..n_rows).map(|i| format!("row_{}", i)).collect::<Vec<_>>());
    let dict_keys = Int32Array::from((0..n_rows).map(|i| (i % 3) as i32).collect::<Vec<_>>());
    let dict_values = StringArray::from(vec!["red", "green", "blue"]);
    let category = DictionaryArray::<Int32Type>::try_new(dict_keys, Arc::new(dict_values)).unwrap();

    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("ids", DataType::Int32, false),
        ArrowField::new("values", DataType::Float64, false),
        ArrowField::new("labels", DataType::Utf8, false),
        ArrowField::new(
            "category",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids) as ArrayRef,
            Arc::new(values),
            Arc::new(labels),
            Arc::new(category),
        ],
    )
    .unwrap()
}

fn narrow_numeric_batch(n_rows: usize) -> RecordBatch {
    let ids = Int32Array::from((0..n_rows as i32).collect::<Vec<_>>());
    let counters = Int64Array::from((0..n_rows).map(|i| (i as i64) * 7).collect::<Vec<_>>());
    let prices = Float32Array::from((0..n_rows).map(|i| i as f32 * 0.25).collect::<Vec<_>>());
    let values = Float64Array::from((0..n_rows).map(|i| i as f64 * 0.5).collect::<Vec<_>>());

    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("ids", DataType::Int32, false),
        ArrowField::new("counters", DataType::Int64, false),
        ArrowField::new("prices", DataType::Float32, false),
        ArrowField::new("values", DataType::Float64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids) as ArrayRef,
            Arc::new(counters),
            Arc::new(prices),
            Arc::new(values),
        ],
    )
    .unwrap()
}

fn string_heavy_batch(n_rows: usize) -> RecordBatch {
    let ids = Int32Array::from((0..n_rows as i32).collect::<Vec<_>>());
    let long_text = StringArray::from(
        (0..n_rows)
            .map(|i| {
                format!(
                    "row_{:08}_payload_{:08x}_lorem_ipsum_dolor_sit",
                    i,
                    i.wrapping_mul(2_654_435_761usize)
                )
            })
            .collect::<Vec<_>>(),
    );
    let short_text = StringArray::from(
        (0..n_rows)
            .map(|i| format!("s_{:04x}", (i & 0xFFFF) as u16))
            .collect::<Vec<_>>(),
    );
    let dict_keys = Int32Array::from(
        (0..n_rows)
            .map(|i| (i % STRING_HEAVY_DICT_CARDINALITY) as i32)
            .collect::<Vec<_>>(),
    );
    let dict_values = StringArray::from(
        (0..STRING_HEAVY_DICT_CARDINALITY)
            .map(|i| format!("cat_{:03}", i))
            .collect::<Vec<_>>(),
    );
    let category = DictionaryArray::<Int32Type>::try_new(dict_keys, Arc::new(dict_values)).unwrap();

    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("ids", DataType::Int32, false),
        ArrowField::new("long_text", DataType::Utf8, false),
        ArrowField::new("short_text", DataType::Utf8, false),
        ArrowField::new(
            "category",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids) as ArrayRef,
            Arc::new(long_text),
            Arc::new(short_text),
            Arc::new(category),
        ],
    )
    .unwrap()
}

fn wide_batch(n_rows: usize) -> RecordBatch {
    let mut fields: Vec<ArrowField> = Vec::with_capacity(WIDE_GROUP_SIZE * 4);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(WIDE_GROUP_SIZE * 4);

    for k in 0..WIDE_GROUP_SIZE {
        fields.push(ArrowField::new(
            format!("i32_{:03}", k),
            DataType::Int32,
            false,
        ));
        let arr = Int32Array::from(
            (0..n_rows)
                .map(|i| (i as i32).wrapping_add(k as i32))
                .collect::<Vec<_>>(),
        );
        columns.push(Arc::new(arr));
    }
    for k in 0..WIDE_GROUP_SIZE {
        fields.push(ArrowField::new(
            format!("i64_{:03}", k),
            DataType::Int64,
            false,
        ));
        let arr = Int64Array::from(
            (0..n_rows)
                .map(|i| (i as i64).wrapping_mul(k as i64 + 1))
                .collect::<Vec<_>>(),
        );
        columns.push(Arc::new(arr));
    }
    for k in 0..WIDE_GROUP_SIZE {
        fields.push(ArrowField::new(
            format!("f32_{:03}", k),
            DataType::Float32,
            false,
        ));
        let arr = Float32Array::from(
            (0..n_rows)
                .map(|i| i as f32 + k as f32 * 0.125)
                .collect::<Vec<_>>(),
        );
        columns.push(Arc::new(arr));
    }
    for k in 0..WIDE_GROUP_SIZE {
        fields.push(ArrowField::new(
            format!("f64_{:03}", k),
            DataType::Float64,
            false,
        ));
        let arr = Float64Array::from(
            (0..n_rows)
                .map(|i| i as f64 + k as f64 * 0.5)
                .collect::<Vec<_>>(),
        );
        columns.push(Arc::new(arr));
    }

    let schema = Arc::new(ArrowSchema::new(fields));
    RecordBatch::try_new(schema, columns).unwrap()
}

/// Minimal Flight service. `DoGet` returns the pre-built batch repeated the
/// number of times the ticket requests as a little-endian `u64`.
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
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info not implemented"))
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
        let n = u64::from_le_bytes(ticket.ticket.as_ref().try_into().unwrap());
        let batch = Arc::clone(&self.batch);
        let batch_stream = stream::iter((0..n).map(move |_| {
            Ok::<RecordBatch, arrow_flight::error::FlightError>((*batch).clone())
        }));
        let flight_data = FlightDataEncoderBuilder::new()
            .with_max_flight_data_size(FLIGHT_MAX_MESSAGE_BYTES)
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
