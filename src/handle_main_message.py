import asyncio
import math
from typing import List, Optional

import aio_pika
import asyncpg
import httpx
import json

from src.ChannelPoolPublisher import ChannelPoolPublisher
from src.custom_logger import logger
from src.postgres_repository import PostgresRepository
from src.messages import YoloInMessage, YoloOutMessage
from src.xml_parser import ProcessingRules



def _to_json_safe_detection(det) -> dict:
    # garante tipos python puros (evita numpy.float32, numpy.int64, etc.)
    confidence = float(det.confidence)
    if math.isnan(confidence) or math.isinf(confidence):
        raise ValueError(f"confidence inválido para JSON: {confidence}")

    image_shape = [int(det.image_shape[0]), int(det.image_shape[1])]

    # mask: se você não tem máscara, manda [] (não manda image_shape aqui)
    mask = det.mask if det.mask is not None else []
    # se mask vier como numpy/qualquer coisa, transforma em algo serializável:
    if not isinstance(mask, list):
        mask = [mask]

    return {
        "network_class_name": str(det.label),
        "id": int(det.id),
        "confidence": confidence,
        "x": float(det.x),
        "y": float(det.y),
        "width": float(det.width),
        "height": float(det.height),
        "image_shape": image_shape,
        "mask": mask,
    }


async def _post_sgc_detection(
    *,
    http_client: httpx.AsyncClient,
    url: str,
    params: dict,
    detections: list[dict],
) -> None:
    # debug: garante que o payload é JSON válido ANTES de enviar
    payload_text = json.dumps(detections, ensure_ascii=False)
    logger.info(f"[POST] {url} payload text: {payload_text}")
    resp = await http_client.post(
        url,
        params=params,
        content=payload_text.encode("utf-8"),  # envia JSON “de verdade”
        headers={"Content-Type": "application/json"},
    )
    resp.raise_for_status()


async def handle_main_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    pg_pool: asyncpg.Pool,
    connection: aio_pika.abc.AbstractRobustConnection,  # (não usado aqui, mas mantive pra compat)
    http_client: httpx.AsyncClient,
    publisher: ChannelPoolPublisher,
    sem: asyncio.Semaphore,
    processing_rules: ProcessingRules,
) -> None:
    """
    Processamento para a MAIN queue (resultado YOLO -> fecha job -> decide próximos jobs -> ou faz rotas finais).

    Ajustes feitos para:
    - Novo schema: finish_job retorna image_pk
    - Context por image_pk: get_image_context_by_image_pk
    - start_job usa image_pk
    - YoloOutMessage tem processed_at (não finished_at)
    """
    async with sem:
        async with message.process(requeue=False):
            try:
                payload = YoloOutMessage.from_message(message=message)
                logger.info(f"[MAIN] YoloOut recebido job_id={payload.job_id}")

                postgres_repository = PostgresRepository(
                    pool=pg_pool,
                    schema=processing_rules.postgres.postgres_consolidator_schema,
                )
                print("repositorio criado")

                async with postgres_repository.pool.acquire() as conn:
                    async with conn.transaction():
                        image_pk = await postgres_repository.init_lock_in_image_data(job_id=payload.job_id,conn=conn)
                        print("image_pk locked",image_pk)
                        # 1) Finaliza o job atual e pega a imagem associada (image_pk interno)
                        image_pk: int = await postgres_repository.finish_job(
                            job_id=payload.job_id,
                            started_at=payload.started_at,
                            finished_at=payload.finished_at,
                            detections = payload.detections,
                            conn=conn
                        )
                        print(f"image_pk={image_pk} job terminado")


            #
                    #     # 2) Contexto atual da imagem (image + jobs + detections)
                        image_context = await postgres_repository.get_image_context_by_image_pk(
                            image_pk=image_pk,
                            conn=conn
                        )
                        print("image_context criado")


                        # 3) Decide próximas redes (se houver)
                        next_networks: List[str] = processing_rules.get_next_network_routes(image_context)

                        print("next_networks",next_networks)

                        for network_name in next_networks:
                            new_job_id = await postgres_repository.start_job(
                                image_pk=image_pk,                 # <- ajuste importante
                                network_name=network_name,
                                network_version="1",               # ou algo do seu rules; antes você usava network_version=network
                                conn=conn
                            )
                            print("new_job_id",new_job_id)

                            yolo_in_message = YoloInMessage(
                                image_url=image_context.image.image_path,  # melhor do que payload.image_url (que nem existe no YoloOut)
                                job_id=new_job_id,
                            )
                            print("yolo_in_message",yolo_in_message)

                            queue_name = processing_rules.routings_config.network_queue_prefix + network_name

                            print("queue_name",queue_name)

                            await publisher.publish(queue_name, yolo_in_message.to_message())

                            print("published")

                        # Se ainda existem redes para rodar, acabou esta execução
                        if next_networks:
                            print("tem redes novas")
                            return
                        print("nenhuma rede nova")

                        # Se não há próximas redes, mas ainda tem job “processing”, não finaliza rotas finais ainda
                        if image_context.has_processing_jobs():
                            print("tem redes processando ainda")
                            return
                        print("nenhuma rede ainda processando")

                        # 4) Rotas finais para SGC
                        final_detections_routes = processing_rules.get_final_detections_routes(image_context)
                        print("final_detections_routes",final_detections_routes)

                        if final_detections_routes:
                            url = processing_rules.sgc.sgc_api_url
                            print("url",url)
                            # Faz em paralelo (bem melhor do que for/await)
                            tasks: List[asyncio.Task] = []
                            for route in final_detections_routes:
                                det = route.detection
                                for target_task in route.target_tasks:
                                    print("target_task",target_task)
                                    # OBS:
                                    # Aqui eu mantive os campos do seu “body” como você já estava montando.
                                    # Pressupõe que route.detection tem:
                                    #   - label / global_class_id / confidence / x / y / width / height / mask (ou compat)
                                    params = {
                                        "target_task": target_task,
                                        "image_path": image_context.image.image_path,
                                        "sender_name": "consolidator",
                                    }


                                    logger.info(f"params: {params}")
                                    detections_payload = [_to_json_safe_detection(det)]
                                    logger.info(f"detections_payload: {detections_payload}")
                                    tasks.append(
                                        asyncio.create_task(
                                            _post_sgc_detection(
                                                http_client=http_client,
                                                url=url,
                                                params=params,
                                                detections=detections_payload,
                                            )
                                        )
                                    )

                            # Se você quiser “fail fast”, troque return_exceptions para False e deixe estourar.
                            await asyncio.gather(*tasks)
                            print("terminou de enviar para o sgc")
                        # 5) Rotas finais para bancos (ex: “Renan”)
                        final_databases_routes = processing_rules.get_final_database_detections(image_context,final_detections_routes)

                        print("final_databases_routes",final_databases_routes)
                        # TODO (ideal): trocar por bulk insert (uma query) dentro do repo.
                        for db_route in final_databases_routes:

                            await postgres_repository.insert_detections_in_final_database(
                                detections=db_route.detections,
                                schema=db_route.Schema,
                                context=image_context,
                                conn=conn
                            )

                        logger.info(f"[MAIN] Finalização completa image_pk={image_pk}")

            except Exception as e:
                #print stack trace
                logger.error(f"[MAIN] Erro de lógica: {e}", exc_info=True)
                raise  # re-levanta para NACK / requeue conforme message.process

            print("unlocked")