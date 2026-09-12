from __future__ import annotations

import asyncio
import logging
import signal

import uvicorn
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage, SimpleEventIsolation

from app.bot.flow_registry import LoginFlowRegistry
from app.bot.handlers import build_router
from app.bot.keyboards import mrkt_result_keyboard, portals_result_keyboard
from app.config import Config
from app.db import Database
from app.jobs import (
    MrktJobService,
    MrktTransferJobService,
    PortalsJobService,
    PortalsTransferBatchService,
    PortalsTransferJobService,
    TonnelTransferBatchService,
    TonnelTransferJobService,
)
from app.telegram_client import (
    AuthService,
    MiniAppService,
    MrktService,
    PortalsService,
    SessionService,
    TonnelService,
)
from app.web import WebAuthCoordinator, create_web_app


async def main() -> None:
    config = Config.load()
    logger = logging.getLogger(__name__)
    logger.info(
        "Starting application web_host=%s web_port=%d mrkt_dry_run=%s "
        "portals_dry_run=%s mrkt_transfer_dry_run=%s portals_transfer_dry_run=%s "
        "mrkt_transfer_mode=%s mrkt_speculative_delay_ms=%d "
        "mrkt_listed_trigger_max_wait_ms=%d mrkt_listed_trigger_poll_interval_ms=%d "
        "max_portals_concurrent_jobs=%d max_tonnel_concurrent_jobs=%d "
        "portals_auto_offer_amount=%s tonnel_auto_offer_amount=%s "
        "tonnel_transfer_dry_run=%s "
        "tonnel_transfer_mode=%s tonnel_api_origin=%s "
        "tonnel_offer_accept_delay_ms=%d "
        "tonnel_ownership_verify_timeout_ms=%d build_name=0020",
        config.web_host,
        config.web_port,
        config.mrkt_dry_run,
        config.portals_dry_run,
        config.mrkt_transfer_dry_run,
        config.portals_transfer_dry_run,
        config.mrkt_transfer_mode,
        config.mrkt_speculative_buy_delay_ms,
        config.mrkt_listed_trigger_max_wait_ms,
        config.mrkt_listed_trigger_poll_interval_ms,
        config.max_portals_concurrent_jobs,
        config.max_tonnel_concurrent_jobs,
        format(config.portals_auto_offer_amount, "f"),
        format(config.tonnel_auto_offer_amount, "f"),
        config.tonnel_transfer_dry_run,
        config.tonnel_transfer_mode,
        config.tonnel_api_origin,
        config.tonnel_offer_accept_delay_ms,
        config.tonnel_ownership_verify_timeout_ms,
    )
    db = Database(config.database_path)
    await db.initialize()
    recovered_jobs = await db.recover_running_mrkt_jobs()
    if recovered_jobs:
        logging.getLogger(__name__).warning(
            "Marked interrupted MRKT jobs ambiguous count=%d", recovered_jobs
        )
    recovered_portals_jobs = await db.recover_running_portals_jobs()
    if recovered_portals_jobs:
        logging.getLogger(__name__).warning(
            "Marked interrupted Portals jobs ambiguous count=%d",
            recovered_portals_jobs,
        )
    recovered_transfer_jobs = await db.recover_running_transfer_jobs()
    if recovered_transfer_jobs:
        logging.getLogger(__name__).warning(
            "Marked interrupted transfer jobs ambiguous count=%d",
            recovered_transfer_jobs,
        )
    recovered_transfer_batches = await db.recover_portals_transfer_batches()
    if recovered_transfer_batches:
        logger.warning(
            "Marked interrupted Portals transfer batches terminal count=%d",
            recovered_transfer_batches,
        )
    recovered_tonnel_batches = await db.recover_tonnel_transfer_batches()
    if recovered_tonnel_batches:
        logger.warning(
            "Marked interrupted Tonnel transfer batches terminal count=%d",
            recovered_tonnel_batches,
        )

    sessions = SessionService(config.sessions_dir)
    sessions.cleanup_unregistered(await db.list_session_keys())
    auth_service = AuthService(config.telegram_api_id, config.telegram_api_hash)
    miniapp_service = MiniAppService(
        config.telegram_api_id,
        config.telegram_api_hash,
        db,
        sessions,
    )
    mrkt_service = MrktService(
        miniapp_service,
        dry_run=config.mrkt_dry_run,
    )
    portals_service = PortalsService(
        miniapp_service,
        dry_run=config.portals_dry_run,
    )
    tonnel_service = TonnelService(
        miniapp_service,
        db,
        dry_run=config.tonnel_transfer_dry_run,
        transfer_mode=config.tonnel_transfer_mode,
        api_origin=config.tonnel_api_origin,
        offer_accept_delay_ms=config.tonnel_offer_accept_delay_ms,
        ownership_verify_timeout_ms=config.tonnel_ownership_verify_timeout_ms,
    )
    login_flows = LoginFlowRegistry()

    bot = Bot(
        token=config.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    async def notify_mrkt_job(owner_telegram_id: int, text: str) -> None:
        await bot.send_message(
            owner_telegram_id,
            text,
            reply_markup=mrkt_result_keyboard(),
        )

    async def notify_portals_job(owner_telegram_id: int, text: str) -> None:
        await bot.send_message(
            owner_telegram_id,
            text,
            reply_markup=portals_result_keyboard(),
        )

    mrkt_jobs = MrktJobService(db, mrkt_service, notifier=notify_mrkt_job)
    portals_jobs = PortalsJobService(db, portals_service, notifier=notify_portals_job)
    mrkt_transfer_jobs = MrktTransferJobService(
        db,
        mrkt_service,
        dry_run=config.mrkt_transfer_dry_run,
        live_verify=config.live_verify,
        speculative_buy=config.mrkt_speculative_buy,
        speculative_buy_delay_ms=config.mrkt_speculative_buy_delay_ms,
        transfer_mode=config.mrkt_transfer_mode,
        listed_trigger_max_wait_ms=config.mrkt_listed_trigger_max_wait_ms,
        listed_trigger_poll_interval_ms=(
            config.mrkt_listed_trigger_poll_interval_ms
        ),
    )
    portals_transfer_jobs = PortalsTransferJobService(
        db,
        portals_service,
        dry_run=config.portals_transfer_dry_run,
        live_verify=config.live_verify,
        offer_amount=config.portals_auto_offer_amount,
    )
    tonnel_transfer_jobs = TonnelTransferJobService(
        db,
        tonnel_service,
        dry_run=config.tonnel_transfer_dry_run,
        transfer_mode=config.tonnel_transfer_mode,
    )
    portals_transfer_batches = PortalsTransferBatchService(
        db,
        portals_transfer_jobs,
        max_concurrency=config.max_portals_concurrent_jobs,
    )
    tonnel_transfer_batches = TonnelTransferBatchService(
        db,
        tonnel_service,
        tonnel_transfer_jobs,
        offer_amount=config.tonnel_auto_offer_amount,
        max_concurrency=config.max_tonnel_concurrent_jobs,
    )
    web_auth = WebAuthCoordinator(
        bot=bot,
        db=db,
        sessions=sessions,
        auth_service=auth_service,
        login_flows=login_flows,
    )
    web_app = create_web_app(config, web_auth)
    web_server = uvicorn.Server(
        uvicorn.Config(
            web_app,
            host=config.web_host,
            port=config.web_port,
            log_level="info",
            access_log=True,
        )
    )

    dispatcher = Dispatcher(
        storage=MemoryStorage(),
        events_isolation=SimpleEventIsolation(),
    )
    dispatcher.include_router(build_router())
    polling_task = asyncio.create_task(
        dispatcher.start_polling(
            bot,
            config=config,
            db=db,
            sessions=sessions,
            auth_service=auth_service,
            miniapp_service=miniapp_service,
            mrkt_service=mrkt_service,
            mrkt_jobs=mrkt_jobs,
            portals_service=portals_service,
            portals_jobs=portals_jobs,
            mrkt_transfer_jobs=mrkt_transfer_jobs,
            portals_transfer_jobs=portals_transfer_jobs,
            portals_transfer_batches=portals_transfer_batches,
            tonnel_service=tonnel_service,
            tonnel_transfer_jobs=tonnel_transfer_jobs,
            tonnel_transfer_batches=tonnel_transfer_batches,
            web_auth=web_auth,
            close_bot_session=False,
            handle_signals=False,
        ),
        name="aiogram-polling",
    )
    web_task = asyncio.create_task(web_server.serve(), name="fastapi-server")
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed_signals: list[signal.Signals] = []
    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(shutdown_signal, shutdown_event.set)
            installed_signals.append(shutdown_signal)
        except (NotImplementedError, RuntimeError):
            pass
    signal_task = asyncio.create_task(shutdown_event.wait(), name="shutdown-signal")

    try:
        done, _ = await asyncio.wait(
            {polling_task, web_task, signal_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if signal_task in done:
            logger.info("Shutdown signal received")
        for task in done:
            if task is signal_task or task.cancelled():
                continue
            exception = task.exception()
            if exception is not None:
                raise exception
    finally:
        signal_task.cancel()
        for shutdown_signal in installed_signals:
            loop.remove_signal_handler(shutdown_signal)
        web_server.should_exit = True
        if not polling_task.done():
            try:
                await dispatcher.stop_polling()
            except RuntimeError:
                polling_task.cancel()
        await asyncio.gather(polling_task, web_task, return_exceptions=True)
        await web_auth.shutdown()
        await login_flows.shutdown()
        await auth_service.shutdown()
        await mrkt_jobs.shutdown()
        await portals_jobs.shutdown()
        await mrkt_transfer_jobs.shutdown()
        await portals_transfer_batches.shutdown()
        await portals_transfer_jobs.shutdown()
        await mrkt_service.shutdown()
        await portals_service.shutdown()
        await bot.session.close()
        logger.info("Application stopped")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("telethon").setLevel(logging.CRITICAL)
    asyncio.run(main())
