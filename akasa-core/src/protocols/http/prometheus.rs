use std::sync::Arc;

use axum::{extract::State, response::IntoResponse};
use lazy_static::lazy_static;
use prometheus::{register_int_gauge, Encoder, IntGauge, TextEncoder};

use crate::GlobalState;

lazy_static! {
    static ref ONLINE_CLIENTS: IntGauge =
        register_int_gauge!("ONLINE_CLIENTS", "Count of online clients").unwrap();
}

#[utoipa::path(
    path = "/metrics",
    get,
    tags = ["metrics"],
    responses((status = OK)),
)]
pub async fn prometheus_metrics(State(global): State<Arc<GlobalState>>) -> impl IntoResponse {
    ONLINE_CLIENTS.set(global.online_clients_count() as i64);

    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();

    ([("content-type", "text/plain")], buffer)
}
