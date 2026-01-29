import asyncio
import logging
import aio_pika
from aio_pika.abc import (
    AbstractRobustConnection,
    AbstractRobustChannel,
    AbstractRobustExchange,
)

logger = logging.getLogger(__name__)


class ChannelPoolPublisher:
    """
    Publisher com Pool, Retry Automático e (opcional) Garantia de Fila (Lazy Declaration).

    ensure_queue:
      - True  -> olha cache; se não existe no cache, declara a fila (durable=True)
      - False -> não verifica nem declara fila; apenas publica
    """

    def __init__(
        self,
        connection: AbstractRobustConnection,
        pool_size: int = 10,
        max_retries: int = 3,
        ensure_queue: bool = True,
    ) -> None:
        if pool_size <= 0:
            raise ValueError("pool_size deve ser > 0")
        if max_retries < 0:
            raise ValueError("max_retries deve ser >= 0")

        self._connection = connection
        self._pool_size = pool_size
        self._max_retries = max_retries
        self._ensure_queue = ensure_queue

        self._pool: asyncio.Queue[
            tuple[AbstractRobustChannel, AbstractRobustExchange]
        ] = asyncio.Queue(maxsize=pool_size)

        # Cache para filas já declaradas (por processo)
        self._declared_queues: set[str] = set()

        self._closed = False

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def start(self) -> None:
        """Inicializa o pool."""
        if self._pool.qsize() > 0:
            return

        logger.info(
            f"Inicializando Publisher "
            f"(size={self._pool_size}, ensure_queue={self._ensure_queue})..."
        )

        for _ in range(self._pool_size):
            await self._create_and_push_channel()

    async def close(self) -> None:
        self._closed = True
        logger.info("Fechando Publisher...")

        while not self._pool.empty():
            try:
                ch, _ = self._pool.get_nowait()
                if not ch.is_closed:
                    await ch.close()
            except Exception:
                pass

    async def _create_and_push_channel(self) -> None:
        if self._closed:
            return

        try:
            ch = await self._connection.channel()
            self._pool.put_nowait((ch, ch.default_exchange))
        except Exception as e:
            logger.error(f"Erro crítico criando canal: {e}")
            raise

    async def publish(
        self,
        queue_name: str,
        message: aio_pika.Message,
        *,
        timeout: int = 10,
    ) -> None:
        """
        Publica na default exchange usando routing_key=queue_name.
        """
        if self._closed:
            raise RuntimeError("Publisher fechado.")

        attempt = 0
        last_error: Exception | None = None

        while attempt <= self._max_retries:
            attempt += 1

            channel, exchange = await self._pool.get()
            return_channel_to_pool = True

            try:
                if channel.is_closed:
                    logger.warning(f"[Try {attempt}] Canal morto. Substituindo...")
                    return_channel_to_pool = False
                    await self._create_and_push_channel()
                    attempt -= 1
                    continue

                # =========================================================
                # (OPCIONAL) GARANTIA DE EXISTÊNCIA DA FILA
                # =========================================================
                if self._ensure_queue and queue_name not in self._declared_queues:
                    await channel.declare_queue(queue_name, durable=True)
                    self._declared_queues.add(queue_name)

                await exchange.publish(
                    message,
                    routing_key=queue_name,
                    timeout=timeout,
                )
                return

            except aio_pika.exceptions.AMQPError as e:
                last_error = e
                return_channel_to_pool = False
                logger.error(f"[Try {attempt}] Erro AMQP: {e}")

                try:
                    await self._create_and_push_channel()
                except Exception:
                    pass

            except Exception as e:
                logger.error(f"Erro genérico no publish: {e}")
                raise

            finally:
                if return_channel_to_pool:
                    self._pool.put_nowait((channel, exchange))

        raise RuntimeError(
            f"Falha ao publicar em '{queue_name}' após {self._max_retries + 1} tentativas. "
            f"Erro: {last_error}"
        )
