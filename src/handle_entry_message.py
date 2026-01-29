import aio_pika
import asyncio
import asyncpg
import httpx

from src.ChannelPoolPublisher import ChannelPoolPublisher
from src.custom_logger import logger
from src.postgres_repository import PostgresRepository
from src.messages import YoloInMessage, ConsolidatorEntryMessage
from src.xml_parser import ProcessingRules


async def handle_entry_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    pg_pool: asyncpg.Pool,
    connection: aio_pika.abc.AbstractRobustConnection,
    http_client: httpx.AsyncClient,
    publisher: ChannelPoolPublisher,
    sem: asyncio.Semaphore,
    processing_rules: ProcessingRules
) -> None:
    """
    ENTRY queue (atualizado):
    - parseia ConsolidatorEntryMessage
    - salva image_data tipada (inclui jetson_detections JSONB)
    - se já existir: ACK e sai (não dispara pipeline)
    - se for nova: cria jobs das próximas redes e publica YoloInMessage
    """
    async with sem:
        async with message.process(requeue=False):
            try:
                payload = ConsolidatorEntryMessage.from_message(message=message)

                logger.info(f"[ENTRY] image_id={payload.image_id} image_path={payload.image_path}")

                postgres_repository = PostgresRepository(
                    pool=pg_pool,
                    schema=processing_rules.postgres.postgres_consolidator_schema
                )


                async with postgres_repository.pool.acquire() as conn:
                    async with conn.transaction():

                        image_pk, inserted_new = await postgres_repository.create_image_data_from_entry(
                            payload=payload,
                            conn=conn,
                        )

                        if not inserted_new:
                            logger.info(f"[ENTRY] Já existe no banco (image_pk={image_pk}). ACK e ignora.")
                            return

                        logger.info(f"[ENTRY] Nova imagem inserida (image_pk={image_pk}). Disparando pipeline...")

                        next_networks = processing_rules.get_next_network_routes()
                        logger.info(f"[ENTRY] Next networks: {next_networks}")

                        for network in next_networks:
                            job_id = await postgres_repository.start_job(
                                image_pk=image_pk,
                                network_name=network,
                                network_version=network,  # se você usa version igual ao nome, mantém
                                conn=conn,
                            )

                            yolo_in_message = YoloInMessage(
                                image_url=payload.image_path,  # agora é image_path (url)
                                job_id=job_id,
                            )

                            queue_name = processing_rules.routings_config.network_queue_prefix + network
                            logger.info(f"[ENTRY] Publish -> queue={queue_name} job_id={job_id}")

                            await publisher.publish(queue_name, yolo_in_message.to_message())

                logger.info("[ENTRY] Pipeline publicado com sucesso.")

            except Exception as e:
                logger.error(f"[ENTRY] Erro: {e}", exc_info=True)
                raise
