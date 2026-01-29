#
# async def send_to_queues(
#     networks_to_inference: List[str],
#     payload: dict,
#     default_exchange: aio_pika.Exchange,
#     settings: Settings,
# ) -> None:
#     if not networks_to_inference:
#         return
#
#     for net in networks_to_inference:
#         queue_name = f"{settings.network_queue_prefix}{net}"
#         new_payload = {
#             "image_id": payload.get("image_id"),
#             "tags": {
#                 "device": payload["tags"].get("device"),
#                 "device_id": payload["tags"]["device_id"],
#                 "camera_id": payload["tags"].get("camera_id"),
#                 "ibge_code": payload["tags"]["ibge_code"],
#             },
#             "image_path": payload["image_path"],
#             "jetson_data": {
#                 "latitude": payload["jetson_data"]["latitude"],
#                 "longitude": payload["jetson_data"]["longitude"],
#                 "timestamp": payload["jetson_data"]["timestamp"],
#                 "detections": [],
#             },
#         }
#
#         msg = aio_pika.Message(
#             body=json.dumps(new_payload).encode("utf-8"),
#             content_type="application/json",
#             delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
#         )
#
#         await default_exchange.publish(msg, routing_key=queue_name)
#         logger.info(f"[RABBIT] published to {queue_name} (net={net})")
#
#
#
# def decode_payload(body: bytes) -> dict:
#     try:
#         decoded = body.decode("utf-8")
#     except Exception:
#         decoded = str(body)
#     try:
#         payload = json.loads(decoded)
#         if isinstance(payload, dict):
#             return payload
#         raise ValueError("payload JSON is not an object")
#     except Exception:
#         raise ValueError(f"Invalid JSON payload: {decoded[:5000]}")
#
#
