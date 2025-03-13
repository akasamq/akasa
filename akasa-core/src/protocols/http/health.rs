use std::sync::Arc;

use axum::{extract::State, response::IntoResponse, Json};
use serde_json::json;

use crate::GlobalState;

#[utoipa::path(
    path = "/health",
    get,
    responses((status = OK)),
)]
pub async fn health(State(_global): State<Arc<GlobalState>>) -> impl IntoResponse {
    Json(json!({
        "status": "ok",
    }))
}
