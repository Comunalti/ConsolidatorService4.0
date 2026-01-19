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
import logging
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


# ======================================================================================
# Settings
# ======================================================================================

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # RabbitMQ
    rabbit_url: str = Field(alias="RABBIT_URL")
    rabbit_queue: str = Field(default="consolidator_queue", alias="RABBIT_QUEUE")
    rabbit_prefetch: int = Field(default=50, alias="RABBIT_PREFETCH")
    worker_concurrency: int = Field(default=20, alias="WORKER_CONCURRENCY")

    # Postgres (main)
    pg_dsn: str = Field(alias="PG_DSN")
    pg_schema: str = Field(default="consolidator", alias="PG_SCHEMA")

    # XML rules
    xml_rules_path: str = Field(alias="XML_RULES_PATH")

    # SGC
    sgc_api_url: str = Field(default="http://10.15.12.1:8880", alias="SGC_API_URL")
    sgc_http_timeout_s: int = Field(default=30, alias="SGC_HTTP_TIMEOUT_S")

    # Queue naming
    network_queue_prefix: str = Field(default="fila_", alias="NETWORK_QUEUE_PREFIX")

    # Optional destination DB DSNs (for final routes)
    final_db_dsn_map_json: str = Field(default="{}", alias="FINAL_DB_DSN_MAP_JSON")

    def final_db_map(self) -> Dict[str, str]:
        try:
            parsed = json.loads(self.final_db_dsn_map_json or "{}")
            if not isinstance(parsed, dict):
                return {}
            # force str-str
            return {str(k): str(v) for k, v in parsed.items()}
        except Exception:
            return {}


# ======================================================================================
# External domain imports (kept as-is, but service is outside Airflow)
# ======================================================================================

# If you need to add your include folder, do it via PYTHONPATH or sys.path.
# Example:
#   export PYTHONPATH=/opt/airflow/include:$PYTHONPATH
#
# Keeping the same types you already use:
from xml_parser import (  # type: ignore
    ImageContext,
    ImageData,
    NetworkStatus,
    Detection,
    ProcessingRules,
)


# ======================================================================================
# Logging
# ======================================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("consolidator_service")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Reduce noisy libs
    logging.getLogger("aio_pika").setLevel(logging.WARNING)
    logging.getLogger("aiormq").setLevel(logging.WARNING)
    logging.getLogger("asyncpg").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    return logger


logger = setup_logging()


# ======================================================================================
# DB helpers (asyncpg)
# ======================================================================================

async def insert_image_into_db(payload: dict, schema: str, conn: asyncpg.Connection) -> None:
    sql = f"""
        INSERT INTO {schema}.image_data(
            image_id, image_path, device_id, ibge_code, latitude, longitude, created_at
        )
        VALUES (
            DEFAULT, $1, $2, $3, $4, $5, $6
        )
        ON CONFLICT (image_path) DO NOTHING
        RETURNING image_id;
    """
    vals = (
        payload["image_path"],
        payload["tags"]["device_id"],
        payload["tags"]["ibge_code"],
        float(payload["jetson_data"]["latitude"]),
        float(payload["jetson_data"]["longitude"]),
        payload["jetson_data"]["timestamp"],  # keep same format your DB expects
    )
    row = await conn.fetchrow(sql, *vals)
    if row:
        logger.info(f"[DB] image_data inserted image_id={row['image_id']} path={payload['image_path']}")
    else:
        logger.info(f"[DB] image_data already exists (ON CONFLICT) path={payload['image_path']}")


async def lock_image_row(payload: dict, schema: str, conn: asyncpg.Connection) -> int:
    sql = f"""
        SELECT image_id
        FROM {schema}.image_data
        WHERE image_path = $1
        FOR UPDATE;
    """
    row = await conn.fetchrow(sql, payload["image_path"])
    if not row:
        raise RuntimeError(f"Image not found for FOR UPDATE: {payload['image_path']}")
    image_id = int(row["image_id"])
    logger.info(f"[DB] [LOCK] image_id={image_id} locked (FOR UPDATE)")
    return image_id


async def get_network_status(payload: dict, schema: str, conn: asyncpg.Connection) -> List[asyncpg.Record]:
    sql = f"""
        SELECT ns.*
        FROM {schema}.network_status AS ns
        INNER JOIN {schema}.image_data AS id
        ON ns.image_id = id.image_id
        WHERE id.image_path = $1;
    """
    rows = await conn.fetch(sql, payload["image_path"])
    return rows


async def get_image_detections(payload: dict, schema: str, conn: asyncpg.Connection) -> List[asyncpg.Record]:
    sql = f"""
        SELECT det.*
        FROM {schema}.detections AS det
        INNER JOIN {schema}.image_data AS id
        ON det.image_id = id.image_id
        WHERE id.image_path = $1;
    """
    rows = await conn.fetch(sql, payload["image_path"])
    return rows


async def insert_detections_into_consolidator_db(
    detections: List[Detection],
    schema: str,
    conn: asyncpg.Connection,
) -> None:
    sql = f"""
        INSERT INTO {schema}.detections(
            image_id, global_class_id, network_name, confidence,
            x, y, width, height, mask, created_at
        )
        VALUES (
            $1, $2, $3, $4,
            $5, $6, $7, $8, $9, $10
        )
        ON CONFLICT DO NOTHING;
    """
    now = datetime.now()
    # batch insert
    args: List[Tuple[Any, ...]] = []
    for det in detections:
        args.append(
            (
                det.image_id,
                det.global_class_id,
                det.network_name,
                det.confidence,
                det.x,
                det.y,
                det.width,
                det.height,
                json.dumps(det.mask),
                now,
            )
        )
    if args:
        await conn.executemany(sql, args)
        logger.info(f"[DB] inserted/ignored detections batch size={len(args)}")


async def update_network_status(payload: dict, schema: str, conn: asyncpg.Connection) -> None:
    sender_name = payload.get("message_sender_info", {}).get("sender_name", "CityLocator")

    sql = f"""
        UPDATE {schema}.network_status ns
        SET net_status = 'done', updated_at = $1
        FROM {schema}.image_data id
        WHERE ns.image_id = id.image_id
          AND id.image_path = $2
          AND ns.network_name = $3;
    """
    await conn.execute(sql, datetime.now(), payload["image_path"], sender_name)
    logger.info(f"[DB] network_status updated to done for sender={sender_name} path={payload['image_path']}")


async def insert_new_networks(payload: dict, networks_to_inference: List[str], schema: str, conn: asyncpg.Connection) -> None:
    if not networks_to_inference:
        return

    sql = f"""
        INSERT INTO {schema}.network_status (
            image_id,
            network_name,
            net_status
        )
        SELECT
            id.image_id  AS image_id,
            $1           AS network_name,
            $2           AS net_status
        FROM {schema}.image_data id
        WHERE id.image_path = $3
        ON CONFLICT DO NOTHING;
    """
    args = [(net, "pending", payload["image_path"]) for net in networks_to_inference]
    await conn.executemany(sql, args)
    logger.info(f"[DB] inserted/ignored new pending networks count={len(networks_to_inference)}")


# ======================================================================================
# Build image context (sync CPU-only object building)
# ======================================================================================

def build_image_context(payload: dict, network_status_db: List[asyncpg.Record], detections_db: List[asyncpg.Record]) -> ImageContext:
    image_context = ImageContext()
    image_id_from_db = 0

    sender_name = payload.get("message_sender_info", {}).get("sender_name", "CityLocator")

    # network status
    for row in network_status_db:
        # row is consolidator.network_status columns
        # expecting: net_status_id, image_id, network_name, net_status, updated_at, ...
        network_name = row["network_name"]
        net_status = "done" if network_name == sender_name else row["net_status"]

        ns = NetworkStatus(
            net_status_id=int(row["net_status_id"]),
            image_id=int(row["image_id"]),
            network_name=str(network_name),
            net_status=str(net_status),
            updated_at=row.get("updated_at"),
        )
        image_id_from_db = int(row["image_id"])
        image_context.network_status.append(ns)

    # image data
    image_obj = ImageData(
        image_id=image_id_from_db,
        image_path=payload["image_path"],
        ibge_code=int(payload["tags"]["ibge_code"]),
        latitude=float(payload["jetson_data"]["latitude"]),
        longitude=float(payload["jetson_data"]["longitude"]),
        created_at=datetime.strptime(payload["jetson_data"]["timestamp"], "%Y-%m-%d %H:%M:%S"),
        inserted_at=datetime.now(),
    )
    image_context.image = image_obj

    # detections from DB
    for row in detections_db:
        det = Detection(
            detection_id=int(row["detection_id"]),
            image_id=int(row["image_id"]),
            global_class_id=int(row["global_class_id"]),
            network_name=str(row["network_name"]),
            confidence=float(row["confidence"]),
            x=float(row["x"]),
            y=float(row["y"]),
            width=float(row["width"]),
            height=float(row["height"]),
            mask=row.get("mask"),
            created_at=row.get("created_at"),
        )
        image_context.detections.append(det)

    # detections from payload
    for net_data in payload.get("network_data", []) or []:
        for d in net_data.get("detections", []) or []:
            det = Detection(
                detection_id=0,
                image_id=image_id_from_db,
                global_class_id=int(d["id"]),
                network_name=str(net_data["network_name"]),
                confidence=float(d["confidence"]),
                x=float(d["x"]),
                y=float(d["y"]),
                width=float(d["width"]),
                height=float(d["height"]),
                mask=d.get("mask", []),
                created_at=datetime.now(),
            )
            image_context.detections.append(det)

    return image_context


# ======================================================================================
# Rabbit publish helpers (aio-pika)
# ======================================================================================

async def send_to_queues(
    networks_to_inference: List[str],
    payload: dict,
    default_exchange: aio_pika.Exchange,
    settings: Settings,
) -> None:
    if not networks_to_inference:
        return

    for net in networks_to_inference:
        queue_name = f"{settings.network_queue_prefix}{net}"
        new_payload = {
            "image_id": payload.get("image_id"),
            "tags": {
                "device": payload["tags"].get("device"),
                "device_id": payload["tags"]["device_id"],
                "camera_id": payload["tags"].get("camera_id"),
                "ibge_code": payload["tags"]["ibge_code"],
            },
            "image_path": payload["image_path"],
            "jetson_data": {
                "latitude": payload["jetson_data"]["latitude"],
                "longitude": payload["jetson_data"]["longitude"],
                "timestamp": payload["jetson_data"]["timestamp"],
                "detections": [],
            },
        }

        msg = aio_pika.Message(
            body=json.dumps(new_payload).encode("utf-8"),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )

        await default_exchange.publish(msg, routing_key=queue_name)
        logger.info(f"[RABBIT] published to {queue_name} (net={net})")


# ======================================================================================
# SGC (async HTTP)
# ======================================================================================

async def send_detection_to_sgc(
    http_client: httpx.AsyncClient,
    settings: Settings,
    image_context: ImageContext,
    detection: Detection,
    task_name: str,
) -> None:
    endpoint = f"{settings.sgc_api_url}/send_detection_to_task"
    params = {
        "target_task": task_name,
        "image_path": image_context.image.image_path,
        "sender_name": detection.network_name,
    }
    payload = [
        {
            "network_class_name": detection.network_name,
            "id": detection.global_class_id,
            "confidence": detection.confidence,
            "x": detection.x,
            "y": detection.y,
            "width": detection.width,
            "height": detection.height,
            "image_shape": [0, 0],
            "mask": detection.mask,
        }
    ]
    r = await http_client.post(endpoint, params=params, json=payload)
    logger.info(f"[SGC] sent task={task_name} status={r.status_code} image_path={image_context.image.image_path}")


# ======================================================================================
# Final routes (parallel SGC + DB destinations)
# ======================================================================================

@dataclass
class DestinationPools:
    pools: Dict[str, asyncpg.Pool]

    async def get_pool(self, name: str, dsn: str) -> asyncpg.Pool:
        if name in self.pools:
            return self.pools[name]
        pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5)
        self.pools[name] = pool
        return pool

    async def close(self) -> None:
        for pool in self.pools.values():
            try:
                await pool.close()
            except Exception:
                pass


async def insert_detections_into_postgres_db(
    image_context: ImageContext,
    detections: List[Detection],
    schema: str,
    conn: asyncpg.Connection,
) -> None:
    sql = f"""
        INSERT INTO {schema}.detections(
            image_id, global_class_id, confidence, x, y, width, height, mask
        )
        SELECT
            id.image_id  AS image_id,
            $1           AS global_class_id,
            $2           AS confidence,
            $3           AS x,
            $4           AS y,
            $5           AS width,
            $6           AS height,
            $7           AS mask
        FROM {schema}.image_data id
        WHERE id.image_path = $8
        ON CONFLICT DO NOTHING;
    """
    for det in detections:
        await conn.execute(
            sql,
            det.global_class_id,
            det.confidence,
            det.x,
            det.y,
            det.width,
            det.height,
            json.dumps(det.mask),
            image_context.image.image_path,
        )


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

def decode_payload(body: bytes) -> dict:
    try:
        decoded = body.decode("utf-8")
    except Exception:
        decoded = str(body)
    try:
        payload = json.loads(decoded)
        if isinstance(payload, dict):
            return payload
        raise ValueError("payload JSON is not an object")
    except Exception:
        raise ValueError(f"Invalid JSON payload: {decoded[:5000]}")


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

async def run_service() -> None:
    settings = Settings()

    logger.info("[BOOT] starting consolidator service (standalone)")
    logger.info(f"[BOOT] queue={settings.rabbit_queue} concurrency={settings.worker_concurrency} prefetch={settings.rabbit_prefetch}")
    logger.info(f"[BOOT] pg_schema={settings.pg_schema} xml_rules={settings.xml_rules_path}")

    # Load rules once
    processing_rules = ProcessingRules(settings.xml_rules_path)
    logger.info("[BOOT] ProcessingRules loaded")

    # Postgres pool (main)
    pg_pool = await asyncpg.create_pool(dsn=settings.pg_dsn, min_size=1, max_size=max(5, settings.worker_concurrency))

    # Destination pools cache
    dest_pools = DestinationPools(pools={})

    # HTTP client
    http_client = httpx.AsyncClient(timeout=httpx.Timeout(settings.sgc_http_timeout_s))

    # Rabbit robust connection
    connection = await aio_pika.connect_robust(settings.rabbit_url)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=settings.rabbit_prefetch)

    queue = await channel.declare_queue(settings.rabbit_queue, durable=True)
    default_exchange = channel.default_exchange

    sem = asyncio.Semaphore(settings.worker_concurrency)
    stop_event = asyncio.Event()

    def _stop(*_: Any) -> None:
        logger.warning("[BOOT] stop signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            # Windows compatibility
            signal.signal(sig, lambda *_: _stop())

    logger.info("[BOOT] consuming...")

    async def _on_message(msg: aio_pika.IncomingMessage) -> None:
        # Fire-and-forget task: aio-pika keeps receiving; semaphore caps concurrency.
        task = asyncio.create_task(
            handle_message(
                msg,
                settings=settings,
                pg_pool=pg_pool,
                processing_rules=processing_rules,
                default_exchange=default_exchange,
                http_client=http_client,
                dest_pools=dest_pools,
                sem=sem,
            )
        )
        task.add_done_callback(lambda t: logger.error(f"[MSG] task crashed: {t.exception()}") if t.exception() else None)

    consumer_tag = await queue.consume(_on_message, no_ack=False)

    await stop_event.wait()

    logger.info("[SHUTDOWN] cancelling consumer...")
    try:
        await queue.cancel(consumer_tag)
    except Exception:
        pass

    logger.info("[SHUTDOWN] closing resources...")
    try:
        await http_client.aclose()
    except Exception:
        pass
    try:
        await dest_pools.close()
    except Exception:
        pass
    try:
        await pg_pool.close()
    except Exception:
        pass
    try:
        await channel.close()
    except Exception:
        pass
    try:
        await connection.close()
    except Exception:
        pass

    logger.info("[SHUTDOWN] done.")


if __name__ == "__main__":
    try:
        asyncio.run(run_service())
    except KeyboardInterrupt:
        pass
