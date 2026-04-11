mod health;
mod prometheus;

use std::sync::Arc;

use axum::{response::Html, routing::get, Json, Router};
use utoipa::OpenApi;

use crate::GlobalState;

fn openapi() -> Router {
    #[derive(OpenApi)]
    #[openapi(info(title = "Akasa"))]
    struct ApiDoc;

    const OPENAPI_ENDPOINT: &str = "/openapi.json";

    Router::new()
        .route(OPENAPI_ENDPOINT, get(||async { Json(ApiDoc::openapi()) }))
        .route("/api", get(|| async {
            Html(format!(
                r#"
                <!doctype html>
                <html>
                <head>
                    <meta charset="utf-8">
                    <script type="module" src="https://unpkg.com/rapidoc/dist/rapidoc-min.js"></script>
                </head>
                <body>
                    <rapi-doc
                        spec-url="{}"
                        theme="light"
                        show-header="false"
                    ></rapi-doc>
                </body>
                </html>
                "#,
                OPENAPI_ENDPOINT
            ))
        }))
}

pub fn get_router(cfg: &crate::config::Http, global: Arc<GlobalState>) -> Router {
    let mut router: Router = Router::new();

    router = router
        .merge(openapi())
        .route("/health", get(health::health));

    if cfg.prometheus {
        router = router.nest(
            "/metrics",
            Router::new().route(
                "/",
                get(prometheus::prometheus_metrics).with_state(Arc::clone(&global)),
            ),
        );
    }

    router
}
