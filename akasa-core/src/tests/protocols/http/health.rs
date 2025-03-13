use std::sync::Arc;

use axum_test::TestServer;
use serde_json::json;

use crate::{
    config::{Config, Http},
    protocols::http::get_router,
    GlobalState,
};

#[tokio::test]
async fn test_health() {
    let config = Config::new_allow_anonymous();
    let global = Arc::new(GlobalState::new(config));
    let cfg = Http::default();
    let app = get_router(&cfg, global);

    let server = TestServer::new(app).unwrap();

    let response = server.get("/health").await;
    response.assert_status_ok();
    response.assert_json(&json!({
        "status": "ok",
    }));
}
