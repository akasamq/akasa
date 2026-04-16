use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::{Service, ServiceExt};

use crate::{
    GlobalState,
    config::{Config, Http},
    protocols::http::get_router,
};

#[tokio::test]
async fn test_health() {
    let config = Config::new_allow_anonymous();
    let global = Arc::new(GlobalState::new(config));
    let cfg = Http::default();
    let mut app = get_router(&cfg, global);

    let request = Request::get("/health").body(Body::empty()).unwrap();
    let response = ServiceExt::<Request<Body>>::ready(&mut app)
        .await
        .unwrap()
        .call(request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        body,
        json!({
            "status": "ok",
        })
    );
}
