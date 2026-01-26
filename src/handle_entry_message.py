import aio_pika
import asyncio
import asyncpg
import httpx

from src.ChannelPoolPublisher import ChannelPoolPublisher
from src.custom_logger import logger
from src.ia_zeladoria_repository import PostgresRepository
from src.messages import YoloInMessage, ConsolidatorEntryMessage
from src.xml_parser import ProcessingRules


async def handle_entry_message(
        message:  aio_pika.abc.AbstractIncomingMessage,
        pg_pool: asyncpg.Pool,
        connection: aio_pika.abc.AbstractRobustConnection,
        http_client: httpx.AsyncClient,
        publisher: ChannelPoolPublisher,
        sem: asyncio.Semaphore,
        processing_rules: ProcessingRules
) -> None:
    """
    Processamento específico para a ENTRY queue.
    """
    async with sem:
        async with message.process(requeue=True):
            try:
                body = message.body.decode()
                logger.info(f"[ENTRY] Processando mensagem: {body[:50]}...")

                payload = ConsolidatorEntryMessage.from_message(message=message)
                logger.info(f"[ENTRY] payload: {payload}")

                postgres_repository = PostgresRepository(
                    pool=pg_pool,
                    schema=processing_rules.postgres.postgres_consolidator_schema
                )
                logger.info(f"[ENTRY] Postgres Repository: {postgres_repository}")

                image_id = await postgres_repository.create_image_data(image_url=payload.image_url)

                logger.info(f"[ENTRY] Image id: {image_id}")
                # image_context = await postgres_repository.get_image_context_by_image_id(image_id=image_id)
                # next_networks = processing_rules.get_next_network_routes(image_context)

                next_networks = processing_rules.get_next_network_routes()
                logger.info(f"[ENTRY] Next networks: {next_networks}")

                for network in next_networks:
                    logger.info(f"[ENTRY] Next network: {network}")
                    job_id = await postgres_repository.start_job(
                        image_id=image_id,
                        network_name=network,
                        network_version=network,
                    )
                    logger.info(f"[ENTRY] Job ID: {job_id}")
                    yolo_in_message = YoloInMessage(
                        image_url=payload.image_url,
                        job_id = job_id,
                    )
                    logger.info(f"[ENTRY] yolo_in_message: {yolo_in_message}")
                    queue_name =processing_rules.routings_config.network_queue_prefix+network
                    logger.info(f"[ENTRY] Queue: {queue_name}")
                    await publisher.publish(
                        queue_name,
                        yolo_in_message.to_message()
                    )
                    logger.info(f"[ENTRY] Message published")

            except Exception as e:
                logger.error(f"[ENTRY] Erro: {e}", exc_info=True)
                raise
