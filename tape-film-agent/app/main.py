from fastapi import FastAPI

from app.config import settings
from app.routes.app_proxy import router as app_proxy_router
from app.routes.health import router as health_router
from app.routes.internal_ops import router as internal_ops_router
from app.routes.shopify_admin import router as shopify_admin_router
from app.routes.shopify_webhooks import router as shopify_webhooks_router
from app.routes.storefront_agent import router as storefront_agent_router


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
    )

    app.include_router(health_router, tags=["health"])
    app.include_router(shopify_webhooks_router, tags=["shopify-webhooks"])
    app.include_router(shopify_admin_router, tags=["shopify-admin"])
    app.include_router(storefront_agent_router, tags=["storefront-agent"])
    app.include_router(app_proxy_router, tags=["app-proxy"])
    app.include_router(internal_ops_router, tags=["internal-ops"])

    return app


app = create_app()
