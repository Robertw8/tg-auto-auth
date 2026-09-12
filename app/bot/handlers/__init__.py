from aiogram import F, Router
from aiogram.enums import ChatType

from .accounts import router as accounts_router
from .auth import router as auth_router
from .errors import handle_error
from .history import router as history_router
from .mrkt_jobs import router as mrkt_jobs_router
from .mrkt_transfers import router as mrkt_transfers_router
from .navigation import router as navigation_router
from .portals_jobs import router as portals_jobs_router
from .portals_offers import router as portals_offers_router
from .portals_transfers import router as portals_transfers_router
from .start import router as start_router
from .tonnel_transfers import router as tonnel_transfers_router


def build_router() -> Router:
    router = Router(name="root")
    router.message.filter(F.chat.type == ChatType.PRIVATE)
    router.callback_query.filter(F.message.chat.type == ChatType.PRIVATE)
    router.errors.register(handle_error)
    router.include_router(start_router)
    router.include_router(accounts_router)
    router.include_router(auth_router)
    router.include_router(mrkt_jobs_router)
    router.include_router(portals_jobs_router)
    router.include_router(portals_offers_router)
    router.include_router(mrkt_transfers_router)
    router.include_router(portals_transfers_router)
    router.include_router(tonnel_transfers_router)
    router.include_router(history_router)
    router.include_router(navigation_router)
    return router


__all__ = ["build_router"]
