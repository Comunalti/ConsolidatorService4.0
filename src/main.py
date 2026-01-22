"""
consolidator_service.py

Standalone RabbitMQ consumer service (outside Airflow) using:
- aio-pika (async, parallel consumption)
- asyncpg (async Postgres)
- httpx (async HTTP to SGC)
- pydantic-settings (.env)

Run:
  python consolidator_service.py

Env (.env) example:

  # --- Rabbit ---
  RABBIT_URL=amqp://rabbitmq:rabbitmq@localhost:5672/
  RABBIT_QUEUE=consolidator_queue
  RABBIT_PREFETCH=50
  WORKER_CONCURRENCY=20

  # --- Postgres (main consolidator DB) ---
  PG_DSN=postgresql://user:pass@localhost:5432/postgres
  PG_SCHEMA=consolidator

  # --- XML rules ---
  XML_RULES_PATH=/opt/airflow/include/routing_rules.xml

  # --- SGC ---
  SGC_API_URL=http://10.15.12.1:8880
  SGC_HTTP_TIMEOUT_S=30

  # --- Per-network queue naming ---
  NETWORK_QUEUE_PREFIX=fila_

  # --- Optional: destination DBs for final routes (map "conn_name" -> dsn) ---
  # FINAL_DB_DSN_MAP_JSON={"PostgreDB":"postgresql://...","OtherDB":"postgresql://..."}
  FINAL_DB_DSN_MAP_JSON={}
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import aio_pika
import asyncpg
import httpx

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from custom_logger import logger
from src.ChannelPoolPublisher import ChannelPoolPublisher
from src.handle_entry_message import handle_entry_message
from src.handle_main_message import handle_main_message

from xml_parser import ProcessingRules, RabbitConfig


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file="../.env", env_file_encoding="utf-8", extra="ignore")


    # XML rules
    xml_rules_path: str = Field(alias="XML_RULES_PATH")



async def process_final_routes_if_needed(
    image_context: ImageContext,
    networks_to_inference: List[str],
    processing_rules: ProcessingRules,
    http_client: httpx.AsyncClient,
    dest_pools: DestinationPools,
    settings: Settings,
) -> None:
    if networks_to_inference:
        logger.info("[FINAL] still has networks_to_inference; skipping final routes")
        return
    if any(ns.net_status == "pending" for ns in image_context.network_status):
        logger.info("[FINAL] still has pending network_status; skipping final routes")
        return

    # These rule calls look sync; keep them sync (fast), but we will parallelize the IO afterwards.
    final_routes = processing_rules.get_final_detections_routes(image_context)
    final_db_map = processing_rules.get_final_database_detections_map(
        image_context,
        final_routes,
    )

    # --- SGC in parallel
    sgc_tasks: List[asyncio.Task] = []
    for det_route in final_routes:
        det = det_route.detection
        if det_route.target_tasks != []:
            task_name = det_route.get_first()
            sgc_tasks.append(
                asyncio.create_task(send_detection_to_sgc(http_client, settings, image_context, det, task_name))
            )

    # --- DB destinations in parallel (per-database transaction)
    db_dsn_map = settings.final_db_map()

    async def _send_to_one_db(database: str, schema: str, detections: List[Detection]) -> None:
        dsn = db_dsn_map.get(database)
        if not dsn:
            logger.warning(f"[FINAL][DB] no DSN configured for database key='{database}' (skip)")
            return
        pool = await dest_pools.get_pool(database, dsn)
        async with pool.acquire() as conn:
            async with conn.transaction():
                await insert_detections_into_postgres_db(image_context, detections, schema, conn)
        logger.info(f"[FINAL][DB] inserted detections into db={database} schema={schema} count={len(detections)}")

    db_tasks: List[asyncio.Task] = []
    for r in final_db_map:
        database = r.database
        detections = r.detections
        schema = r.schema
        if not detections:
            continue
        db_tasks.append(asyncio.create_task(_send_to_one_db(database, schema, detections)))

    # wait all IO
    if sgc_tasks or db_tasks:
        results = await asyncio.gather(*sgc_tasks, *db_tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, Exception):
                # do not crash the whole message; raise so message transaction can rollback (your choice)
                raise res


# ======================================================================================
# Message processing (one message == one DB transaction + ack/nack)
# ======================================================================================


async def handle_message(
    message: aio_pika.IncomingMessage,
    *,
    settings: Settings,
    pg_pool: asyncpg.Pool,
    processing_rules: ProcessingRules,
    default_exchange: aio_pika.Exchange,
    http_client: httpx.AsyncClient,
    dest_pools: DestinationPools,
    sem: asyncio.Semaphore,
) -> None:
    async with sem:
        async with message.process(requeue=True):  # auto-ack on success; requeue on exception
            payload = decode_payload(message.body)
            schema = settings.pg_schema

            logger.info(f"[MSG] received routing_key={message.routing_key} path={payload.get('image_path')}")

            async with pg_pool.acquire() as conn:
                async with conn.transaction():
                    await insert_image_into_db(payload, schema=schema, conn=conn)
                    _ = await lock_image_row(payload, schema=schema, conn=conn)

                    network_status_db, detections_db = await asyncio.gather(
                        get_network_status(payload, schema=schema, conn=conn),
                        get_image_detections(payload, schema=schema, conn=conn),
                    )

                    image_context = build_image_context(payload, network_status_db, detections_db)

                    # next networks (sync)
                    networks_to_inference = processing_rules.get_next_network_routes(image_context)

                    # Insert detections + update status + insert new networks (parallel DB ops)
                    await asyncio.gather(
                        insert_detections_into_consolidator_db(image_context.detections, schema=schema, conn=conn),
                        update_network_status(payload, schema=schema, conn=conn),
                        insert_new_networks(payload, networks_to_inference, schema=schema, conn=conn),
                    )

                    # publish to other queues (rabbit IO)
                    await send_to_queues(networks_to_inference, payload, default_exchange, settings)

                    # final routes IO (SGC + destination DBs)
                    await process_final_routes_if_needed(
                        image_context=image_context,
                        networks_to_inference=networks_to_inference,
                        processing_rules=processing_rules,
                        http_client=http_client,
                        dest_pools=dest_pools,
                        settings=settings,
                    )

            logger.info(f"[MSG] done path={payload.get('image_path')}")


# ======================================================================================
# Service runner
# ======================================================================================
from typing import Tuple
from aio_pika.abc import AbstractRobustConnection, AbstractRobustQueue


async def connect_to_rabbit(
        rabbit: RabbitConfig
) -> tuple[AbstractRobustConnection, AbstractRobustQueue, AbstractRobustQueue, ChannelPoolPublisher]:
    # 1. Conexão robusta
    connection: AbstractRobustConnection = await aio_pika.connect_robust(
        host=rabbit.rabbit_host,
        port=rabbit.rabbit_port,
        login=rabbit.rabbit_user,
        password=rabbit.rabbit_pass
    )

    # 2. Canal e Fila MAIN
    main_channel = await connection.channel()

    await main_channel.set_qos(prefetch_count=rabbit.worker_concurrency)
    main_queue: AbstractRobustQueue = await main_channel.declare_queue(
        rabbit.rabbit_main_queue,
        durable=True
    )

    # 3. Canal e Fila ENTRY
    entry_channel = await connection.channel()
    await entry_channel.set_qos(prefetch_count=rabbit.worker_concurrency)
    entry_queue: AbstractRobustQueue = await entry_channel.declare_queue(
        rabbit.rabbit_entry_queue,
        durable=True
    )

    publisher = ChannelPoolPublisher(connection, pool_size=10)
    await publisher.start()


    return connection, main_queue, entry_queue, publisher


async def run_service() -> None:
    settings = Settings()
    logger.info(f"[BOOT] starting consolidator service with settings={settings}")

    connection = None
    pg_pool = None
    http_client = None

    stop_event = asyncio.Event()

    try:
        # 1. Carregamento de Regras
        processing_rules = ProcessingRules(settings.xml_rules_path)
        logger.info("[BOOT] ProcessingRules loaded")

        # 2. Conexão RabbitMQ
        connection, main_queue, entry_queue, publisher = await connect_to_rabbit(processing_rules.rabbit)
        logger.info(f"[BOOT] RabbitMQ connected. Queues: {main_queue.name}, {entry_queue.name}")

        # 3. Pool do Postgres (ATENÇÃO: adicionei o await que faltava no seu código original)
        pg = processing_rules.postgres
        pg_pool = await asyncpg.create_pool(
            host=pg.postgres_host,
            port=pg.postgres_port,
            user=pg.postgres_user,
            password=pg.postgres_pass,
            database=pg.postgres_database,
            min_size=5,
            max_size=processing_rules.rabbit.worker_concurrency + 2
        )
        logger.info(f"[BOOT] Postgres Pool created")

        # 4. HTTP Client
        http_client = httpx.AsyncClient(timeout=httpx.Timeout(30.0))
        logger.info("[BOOT] HTTP Client initialized")

        # --- Controle de Concorrência ---
        sem = asyncio.Semaphore(processing_rules.rabbit.worker_concurrency)

        # --- Handlers de Sinal ---
        def _stop(*_: Any) -> None:
            logger.warning("[BOOT] stop signal received")
            stop_event.set()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _stop)
            except NotImplementedError:
                signal.signal(sig, lambda *_: _stop())

        # =========================================================================
        # DEFINIÇÃO DOS CALLBACKS (Wrappers)
        # =========================================================================

        # Callback específico para a Fila MAIN
        async def _on_main_message(msg: aio_pika.IncomingMessage) -> None:
            task = asyncio.create_task(
                handle_main_message(
                    message=msg,
                    pg_pool=pg_pool,
                    connection=connection,
                    http_client=http_client,
                    publisher=publisher,
                    sem=sem,
                    processing_rules=processing_rules
                )
            )
            task.add_done_callback(
                lambda t: logger.error(f"[MAIN] task crashed: {t.exception()}")
                if not t.cancelled() and t.exception() else None
            )

        # Callback específico para a Fila ENTRY
        async def _on_entry_message(msg: aio_pika.IncomingMessage) -> None:
            task = asyncio.create_task(
                handle_entry_message(
                    message=msg,
                    pg_pool=pg_pool,
                    connection=connection,
                    http_client=http_client,
                    publisher=publisher,
                    sem=sem,
                    processing_rules=processing_rules
                )
            )
            task.add_done_callback(
                lambda t: logger.error(f"[ENTRY] task crashed: {t.exception()}")
                if not t.cancelled() and t.exception() else None
            )

        # =========================================================================

        # 5. Iniciar Consumo (Ligando cada fila ao seu callback)
        logger.info("[BOOT] consuming from both queues...")

        tag_main = await main_queue.consume(_on_main_message)  # <--- Usa o wrapper Main
        tag_entry = await entry_queue.consume(_on_entry_message)  # <--- Usa o wrapper Entry

        # 6. Aguardar
        await stop_event.wait()

    except Exception as e:
        logger.critical(f"[FATAL] Service crashed: {e}", exc_info=True)
        raise

    finally:
        logger.info("[SHUTDOWN] closing resources...")
        if http_client:
            await http_client.aclose()
        if pg_pool:
            await pg_pool.close()
        if connection:
            await connection.close()
        logger.info("[SHUTDOWN] done.")


if __name__ == "__main__":
    try:
        asyncio.run(run_service())
    except KeyboardInterrupt:
        pass
