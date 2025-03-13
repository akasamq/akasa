use std::sync::Arc;

use axum_test::TestServer;

use crate::{
    config::{Config, Http},
    protocols::http::get_router,
    GlobalState,
};

#[tokio::test]
async fn test_prometheus_metrics() {
    let config = Config::new_allow_anonymous();
    let global = Arc::new(GlobalState::new(config));
    let cfg = Http::default();
    let app = get_router(&cfg, global);

    let server = TestServer::new(app).unwrap();

    let response = server.get("/metrics").await;
    response.assert_status_ok();
    response.assert_text_contains("ONLINE_CLIENTS");
}
