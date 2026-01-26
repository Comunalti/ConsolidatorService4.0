import aio_pika
import asyncio
import asyncpg
import httpx
from httpx import Request

from src.ChannelPoolPublisher import ChannelPoolPublisher
from src.custom_logger import logger
from src.ia_zeladoria_repository import PostgresRepository
from src.messages import YoloInMessage, ConsolidatorEntryMessage, YoloOutMessage
from src.xml_parser import ProcessingRules

async def handle_main_message(
        message:  aio_pika.abc.AbstractIncomingMessage,
        pg_pool: asyncpg.Pool,
        connection: aio_pika.abc.AbstractRobustConnection,
        http_client: httpx.AsyncClient,
        publisher: ChannelPoolPublisher,
        sem: asyncio.Semaphore,
        processing_rules: ProcessingRules
) -> None:
    """
    Processamento específico para a MAIN queue.
    """
    async with sem:
        async with message.process(requeue=True):
            try:
                body = message.body.decode()
                logger.info(f"[ENTRY] Processando mensagem: {body[:50]}...")

                payload = YoloOutMessage.from_message(message=message)
                logger.info(f"[ENTRY] payload: {payload}")

                postgres_repository = PostgresRepository(
                    pool=pg_pool,
                    schema=processing_rules.postgres.postgres_consolidator_schema
                )

                # image_id = await postgres_repository.create_image_data(image_url=payload.image_url)
                image_id = await postgres_repository.finish_job(
                    job_id=payload.job_id,
                    finished_at=payload.finished_at,
                )

                image_context = await postgres_repository.get_image_context_by_image_id(image_id=image_id)
                next_networks = processing_rules.get_next_network_routes(image_context)

                for network in next_networks:
                    job_id = await postgres_repository.start_job(
                        image_id=image_id,
                        network_name=network,
                        network_version=network,
                    )
                    yolo_in_message = YoloInMessage(
                        image_url=payload.image_url,
                        job_id=job_id,
                    )
                    queue_name = processing_rules.routings_config.network_queue_prefix + network
                    await publisher.publish(
                        queue_name,
                        yolo_in_message.to_message()
                    )

                if next_networks:
                    return

                #if not return than no more network must start

                if image_context.has_processing_jobs():
                    return

                # no more jobs in execution

                # this is the last consolidator run for this image

                final_detections_routes = processing_rules.get_final_detections_routes(image_context)

                #todo: fazer em paralelo esses requests
                for final_detections_route in final_detections_routes:

                    detection = final_detections_route.detection

                    url = processing_rules.sgc.sgc_api_url
                    params = {
                        "target_task": final_detections_route.target_task,
                        "image_path":image_context.image.image_path,
                        "sender_name":final_detections_route.sender_name,
                    }
                    body = [
                        {
                            "network_class_name": detection.label,
                            "id": detection.global_class_id,
                            "confidence": detection.confidence,
                            "x": detection.x,
                            "y": detection.y,
                            "width": detection.width,
                            "height": detection.height,
                            "image_shape": [
                                0
                            ],
                            "mask": []
                        }
                    ]
                    try:
                        response = await http_client.post(
                            url,
                            params=params,
                            json=body,  # httpx já serializa pra JSON automaticamente
                        )# Levanta erro se for 4xx ou 5xx
                        response.raise_for_status()
                        logger.debug(f"[SGC] Sucesso: {response.status_code} - {url}")
                    except httpx.HTTPStatusError as e:
                        logger.error(f"[SGC] Erro HTTP {e.response.status_code} enviando para {url}: {e}")
                    # Aqui você decide se quer relançar o erro ou apenas logar
                    except httpx.RequestError as e:
                        logger.error(f"[SGC] Erro de Conexão enviando para {url}: {e}")

                #agora processar para enviar para o renan

                final_databases_routes =processing_rules.get_final_database_detections(image_context)

                #trocar isso pra uma unica query para inserir todas as detecções de uma vez só
                for final_database_route in final_databases_routes:

                    await postgres_repository.insert_detection_in_final_database(
                        detection=final_database_route.detection,
                        schema=final_database_route.Schema
                    )

                    logger.debug(f"[Database Renan] Sucesso: {response.status_code} - {url}")

            except Exception as e:
                logger.error(f"[MAIN] Erro de lógica: {e}")
                raise  # Re-levanta o erro para o message.process dar NACK

