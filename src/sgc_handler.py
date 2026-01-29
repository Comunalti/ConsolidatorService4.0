#
# async def send_detection_to_sgc(
#     http_client: httpx.AsyncClient,
#     settings: Settings,
#     image_context: ImageContext,
#     detection: Detection,
#     task_name: str,
# ) -> None:
#     endpoint = f"{settings.sgc_api_url}/send_detection_to_task"
#     params = {
#         "target_task": task_name,
#         "image_path": image_context.image.image_path,
#         "sender_name": detection.network_name,
#     }
#     payload = [
#         {
#             "network_class_name": detection.network_name,
#             "id": detection.global_class_id,
#             "confidence": detection.confidence,
#             "x": detection.x,
#             "y": detection.y,
#             "width": detection.width,
#             "height": detection.height,
#             "image_shape": [0, 0],
#             "mask": detection.mask,
#         }
#     ]
#     r = await http_client.post(endpoint, params=params, json=payload)
#     logger.info(f"[SGC] sent task={task_name} status={r.status_code} image_path={image_context.image.image_path}")
