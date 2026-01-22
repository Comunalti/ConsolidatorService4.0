from custom_logger import logger


async def handle_main_message(
        message: aio_pika.IncomingMessage,
        pg_pool: asyncpg.Pool,
        connection: aio_pika.abc.AbstractRobustConnection,
        http_client: httpx.AsyncClient,
        sem: asyncio.Semaphore,
        processing_rules: ProcessingRules
) -> None:
    """
    Processamento específico para a MAIN queue.
    """
    async with sem:  # Limita a concorrência
        # O process(requeue=True) garante o ACK se der tudo certo ou NACK se der erro
        async with message.process(requeue=True):
            try:
                # Exemplo: Decodifica
                body = message.body.decode()
                logger.info(f"[MAIN] Processando mensagem: {body[:50]}...")

                # Exemplo: Usando o banco
                async with pg_pool.acquire() as conn:
                    # Logica específica da MAIN aqui
                    pass

                # Exemplo: Usando o Rabbit para publicar algo novo (se precisar)
                # channel = await connection.channel()
                # ... publica ...

            except Exception as e:
                logger.error(f"[MAIN] Erro de lógica: {e}")
                raise  # Re-levanta o erro para o message.process dar NACK

