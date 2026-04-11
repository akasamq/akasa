use axum::Json;

#[derive(utoipa::ToSchema)]
pub struct Health {
    pub status: String,
}

#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "Server status", body = Health),
    ),
)]
pub async fn health() -> Json<Health> {
    Json(Health {
        status: "ok".into(),
    })
}
