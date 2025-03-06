mod health;
mod prometheus;

use std::sync::Arc;

use crate::GlobalState;
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};
#[cfg(feature = "swagger-ui")]
use utoipa_swagger_ui::SwaggerUi;

#[derive(OpenApi)]
#[openapi(info(title = "Akasa"))]
struct ApiDoc;

pub fn get_router(cfg: &crate::config::Http, global: Arc<GlobalState>) -> axum::Router {
    let mut router = OpenApiRouter::with_openapi(ApiDoc::openapi());

    router = router.routes(routes!(super::http::health::health,));

    if cfg.prometheus {
        router = router.routes(routes!(super::http::prometheus::prometheus_metrics));
    }

    let (router, _openapi) = router.split_for_parts();

    #[cfg(feature = "swagger-ui")]
    let router = if cfg.swagger_ui {
        router.merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", _openapi))
    } else {
        router
    };
    router.with_state(Arc::clone(&global))
}
