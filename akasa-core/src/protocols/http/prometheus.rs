use std::sync::Arc;

use axum::extract::State;

use crate::GlobalState;

#[utoipa::path(
    get,
    path = "/metrics",
    tags = ["metrics"],
    responses(
        (status = OK, description = "Prometheus metrics", body = String)
    ),
)]
pub async fn prometheus_metrics(State(global): State<Arc<GlobalState>>) -> String {
    let count = global.online_clients_count();

    format!(
        "# HELP ONLINE_CLIENTS Count of online clients\n\
         # TYPE ONLINE_CLIENTS gauge\n\
         ONLINE_CLIENTS {}\n",
        count
    )
}
