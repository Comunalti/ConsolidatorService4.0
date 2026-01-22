from __future__ import annotations
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Type, TypeVar

import aio_pika
from pydantic import BaseModel, ConfigDict, Field, ValidationError

# Definição de Tipo para o retorno do from_message ser dinâmico
T = TypeVar("T", bound="RabbitMessageModel")


class RabbitMessageModel(BaseModel):
    """
    Classe base que adiciona capacidades de serialização/deserialização
    RabbitMQ para qualquer modelo Pydantic.
    """
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True
    )

    def to_message(self, **kwargs) -> aio_pika.Message:
        """
        Transforma a instância atual em um aio_pika.Message pronto para publicação.
        """
        # Serializa para JSON usando o Pydantic (trata datetimes automaticamente)
        body_bytes = self.model_dump_json().encode("utf-8")

        return aio_pika.Message(
            body=body_bytes,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            **kwargs
        )

    @classmethod
    def from_message(cls: Type[T], message: aio_pika.IncomingMessage) -> T:
        """
        Método estático/classe que lê uma IncomingMessage, valida o JSON
        e retorna a instância da classe correta.

        Raises:
            ValueError: Se o corpo não for um JSON válido.
            ValidationError: Se o JSON não tiver os campos obrigatórios.
        """
        try:
            # Pydantic V2: model_validate_json faz o parse e validação direto
            return cls.model_validate_json(message.body)

        except ValidationError as e:
            # Erro de estrutura (campo faltando, tipo errado)
            raise ValueError(f"Mensagem inválida para {cls.__name__}: {e}") from e
        except Exception as e:
            # Erro de parse (JSON quebrado)
            raise ValueError(f"Corpo da mensagem não é um JSON válido: {e}") from e


# ==============================================================================
# Modelos de Dados
# ==============================================================================

class DetectionModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    pass_detections_id: int
    job_id: int
    class_id: int

    confidence: Optional[float] = None
    x: float
    y: float
    width: float
    height: float

    mask: Optional[Dict[str, Any]] = None


# ==============================================================================
# As 3 Mensagens Solicitadas
# ==============================================================================

class ConsolidatorEntryMessage(RabbitMessageModel):
    """
    Mensagem de entrada inicial.
    Conteúdo: apenas image_url.
    """
    image_url: str


class YoloInMessage(RabbitMessageModel):
    """
    Mensagem enviada para a fila de inferência (YOLO).
    """
    job_id: int
    image_url: str


class YoloOutMessage(RabbitMessageModel):
    """
    Mensagem de resultado do YOLO.
    """
    job_id: int
    started_at: datetime
    processed_at: datetime
    detections: List[DetectionModel] = Field(default_factory=list)