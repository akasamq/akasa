use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::{Service, ServiceExt};

use crate::{
    GlobalState,
    config::{Config, Http},
    protocols::http::get_router,
};

#[tokio::test]
async fn test_prometheus_metrics() {
    let config = Config::new_allow_anonymous();
    let global = Arc::new(GlobalState::new(config));
    let cfg = Http::default();
    let mut app = get_router(&cfg, global);

    let request = Request::get("/metrics").body(Body::empty()).unwrap();
    let response = ServiceExt::<Request<Body>>::ready(&mut app)
        .await
        .unwrap()
        .call(request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let expected = "# HELP ONLINE_CLIENTS Count of online clients\n\
                    # TYPE ONLINE_CLIENTS gauge\n\
                    ONLINE_CLIENTS 0\n";
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], expected.as_bytes());
}
