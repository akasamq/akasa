use axum::Json;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, ToSchema)]
pub struct Health {
    pub status: String,
}

#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = OK, description = "Server status", body = Health),
    ),
)]
pub async fn health() -> Json<Health> {
    Json(Health {
        status: "ok".into(),
    })
}
