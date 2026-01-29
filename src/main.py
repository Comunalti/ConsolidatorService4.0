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
from src.postgres_repository import ImageContextModel

from xml_parser import ProcessingRules, RabbitConfig


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file="../.env", env_file_encoding="utf-8", extra="ignore")


    # XML rules
    xml_rules_path: str = Field(alias="XML_RULES_PATH")



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

    # 2. Canal e Fila ENTRY
    entry_channel = await connection.channel()
    await entry_channel.declare_queue(rabbit.rabbit_entry_dlq, durable=True)
    await entry_channel.set_qos(prefetch_count=rabbit.rabbit_entry_prefetch_count)
    entry_queue: AbstractRobustQueue = await entry_channel.declare_queue(
        rabbit.rabbit_entry_queue,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": rabbit.rabbit_entry_dlq,
        },
    )

    # 3. Canal e Fila MAIN
    main_channel = await connection.channel()
    await main_channel.declare_queue(rabbit.rabbit_main_dlq, durable=True)
    await main_channel.set_qos(prefetch_count=rabbit.rabbit_main_prefetch_count)
    main_queue: AbstractRobustQueue = await main_channel.declare_queue(
        rabbit.rabbit_main_queue,
        durable=True,
        arguments={
            # Usa a exchange default (sem precisar declarar exchange)
            "x-dead-letter-exchange": "",
            # Routing key = nome exato da fila DLQ
            "x-dead-letter-routing-key": rabbit.rabbit_main_dlq,
        },
    )





    publisher = ChannelPoolPublisher(connection, pool_size=rabbit.rabbit_publisher_pool_size)
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
            min_size=1,
            max_size=processing_rules.postgres.postgres_pool_size,
        )
        logger.info(f"[BOOT] Postgres Pool created")

        # 4. HTTP Client
        http_client = httpx.AsyncClient(timeout=httpx.Timeout(30.0))
        logger.info("[BOOT] HTTP Client initialized")

        # --- Controle de Concorrência ---
        sem = asyncio.Semaphore(processing_rules.worker_concurrency)

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
        async def _on_main_message(msg: aio_pika.abc.AbstractIncomingMessage) -> None:
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
        async def _on_entry_message(msg: aio_pika.abc.AbstractIncomingMessage) -> None:
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
