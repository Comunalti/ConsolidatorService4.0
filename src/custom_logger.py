import logging
import sys

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("consolidator_service")
    logger.setLevel(logging.INFO)

    # 🚫 evita adicionar handler duplicado
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # 🚫 impede propagação para o root logger
    logger.propagate = False

    # Reduce noisy libs
    logging.getLogger("aio_pika").setLevel(logging.WARNING)
    logging.getLogger("aiormq").setLevel(logging.WARNING)
    logging.getLogger("asyncpg").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    return logger


logger = setup_logging()
