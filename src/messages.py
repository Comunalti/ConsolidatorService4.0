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
    def from_message(cls: Type[T], message:  aio_pika.abc.AbstractIncomingMessage) -> T:
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

class YoloDetection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    label: str
    id: int
    confidence: float

    x: float
    y: float
    width: float
    height: float

    image_shape: List[int] = Field(min_length=2, max_length=2)
    mask: Optional[List[Any]] = None
# # ==============================================================================
# # Modelos de Dados
# # ==============================================================================
# class DetectionResult(BaseModel):
#     label: str
#     id: int
#     confidence: float
#
#     x: float
#     y: float
#     width: float
#     height: float
#
#     x_min: int
#     y_min: int
#     x_max: int
#     y_max: int
#
#     image_shape: List[int]  # [W, H]
#     mask: Optional[Dict[str, Any]] = None



class DetectionEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    class_id: int
    shape_type: str
    shape: str
    confidence: float


class JetsonData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    latitude: float
    longitude: float
    timestamp: datetime
    detections: List[DetectionEntry]


class ImageTags(BaseModel):
    model_config = ConfigDict(extra="forbid")

    device: str
    device_id: str
    camera_id: str
    ibge_code: str


# ============================================================
# Mensagem principal
# ============================================================

class ConsolidatorEntryMessage(RabbitMessageModel):
    """
    Mensagem de entrada inicial do Consolidator.

    Representa exatamente o JSON recebido da Jetson / Entry Point.
    """
    model_config = ConfigDict(extra="forbid")

    image_id: int
    image_path: str
    tags: ImageTags
    jetson_data: JetsonData

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
    finished_at: datetime
    detections: List[YoloDetection] = Field(default_factory=list)