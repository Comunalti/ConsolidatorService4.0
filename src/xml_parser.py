# xml_parser.py
from __future__ import annotations

from datetime import datetime
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, ClassVar, Dict, Iterator, List, Optional, Type, TypeVar, Union

from pydantic import BaseModel, Field

from ia_zeladoria_repository import (
    ImageContextModel,
    ImageDataModel,
    DetectionModel,
    JobModel,
)

# ================================================================
# Helpers
# ================================================================

def iter_detections(ctx: ImageContextModel) -> Iterator[DetectionModel]:
    for job in ctx.jobs:
        for det in job.detections:
            yield det


# ================================================================
# ROUTING CONDITIONS
# ================================================================

TRoutingCond = TypeVar("TRoutingCond", bound="RoutingCondition")


class RoutingCondition(BaseModel):
    """
    Base para condições usadas pelas regras de roteamento de redes.
    Avalia apenas com base no ImageContextModel.
    """
    model_config = {"arbitrary_types_allowed": True}

    _registry: ClassVar[Dict[str, Type["RoutingCondition"]]] = {}

    @classmethod
    def register_xml_tag(cls, tag: str):
        """Registra subclasses específicas para <routing>."""
        def deco(sub_cls: Type["RoutingCondition"]):
            cls._registry[tag] = sub_cls
            return sub_cls
        return deco

    @classmethod
    def from_xml_node(cls: Type[TRoutingCond], node: ET.Element) -> TRoutingCond:
        tag = node.tag
        if tag not in cls._registry:
            raise ValueError(f"RoutingCondition XML desconhecida: <{tag}>")
        return cls._registry[tag].from_xml(node)  # type: ignore

    def evaluate(self, ctx: ImageContextModel) -> bool:
        raise NotImplementedError()


@RoutingCondition.register_xml_tag("NotProcessedInNetwork")
class RC_NotProcessedInNetwork(RoutingCondition):
    network_name: str

    @classmethod
    def from_xml(cls, node: ET.Element):
        return cls(network_name=node.attrib["network_name"])

    def evaluate(self, ctx: ImageContextModel) -> bool:
        # "não processado" = não existe job daquela rede em processing|finished
        return not any(
            j.network_name == self.network_name and j.status in ("processing", "finished")
            for j in ctx.jobs
        )


@RoutingCondition.register_xml_tag("ProcessingFinishedInNetwork")
class RC_ProcessingFinishedInNetwork(RoutingCondition):
    network_name: str

    @classmethod
    def from_xml(cls, node: ET.Element):
        return cls(network_name=node.attrib["network_name"])

    def evaluate(self, ctx: ImageContextModel) -> bool:
        return any(
            j.network_name == self.network_name and j.status == "finished"
            for j in ctx.jobs
        )


@RoutingCondition.register_xml_tag("RequiresDetectionOfClass")
class RC_RequiresDetectionOfClass(RoutingCondition):
    class_id: int

    @classmethod
    def from_xml(cls, node: ET.Element):
        return cls(class_id=int(node.attrib["class_id"]))

    def evaluate(self, ctx: ImageContextModel) -> bool:
        return any(det.global_class_id == self.class_id for det in iter_detections(ctx))


@RoutingCondition.register_xml_tag("CityIsIbge")
class RC_CityIsIbge(RoutingCondition):
    code: int

    @classmethod
    def from_xml(cls, node: ET.Element):
        return cls(code=int(node.attrib["code"]))

    def evaluate(self, ctx: ImageContextModel) -> bool:
        # No schema novo, ibge_code pode estar em tags (ou você pode ter coluna dedicada no futuro).
        # Aqui seguimos o comportamento antigo: tenta pegar do ctx.image.tags["ibge_code"].
        if not ctx.image:
            return False
        if ctx.image.tags and "ibge_code" in ctx.image.tags:
            try:
                return int(ctx.image.tags["ibge_code"]) == self.code
            except Exception:
                return False
        return False


@RoutingCondition.register_xml_tag("or")
class RC_Or(RoutingCondition):
    conditions: List[RoutingCondition]

    @classmethod
    def from_xml(cls, node: ET.Element):
        sub = [RoutingCondition.from_xml_node(c) for c in node]
        return cls(conditions=sub)

    def evaluate(self, ctx: ImageContextModel) -> bool:
        return any(c.evaluate(ctx) for c in self.conditions)


# ================================================================
# DETECTION CONDITIONS
# ================================================================

TDetCond = TypeVar("TDetCond", bound="DetectionCondition")


class DetectionCondition(BaseModel):
    """
    Base para condições usadas pelas regras de detecção (DetectionsRules).
    Avalia com base no ImageContextModel E na DetectionModel.
    """
    model_config = {"arbitrary_types_allowed": True}

    _registry: ClassVar[Dict[str, Type["DetectionCondition"]]] = {}

    @classmethod
    def register_xml_tag(cls, tag: str):
        def deco(sub_cls: Type["DetectionCondition"]):
            cls._registry[tag] = sub_cls
            return sub_cls
        return deco

    @classmethod
    def from_xml_node(cls: Type[TDetCond], node: ET.Element) -> TDetCond:
        tag = node.tag
        if tag not in cls._registry:
            raise ValueError(f"DetectionCondition XML desconhecida: <{tag}>")
        return cls._registry[tag].from_xml(node)  # type: ignore

    def evaluate(self, ctx: ImageContextModel, det: DetectionModel) -> bool:
        raise NotImplementedError()


@DetectionCondition.register_xml_tag("DetectionIs")
class DC_DetectionIs(DetectionCondition):
    class_id: int

    @classmethod
    def from_xml(cls, node: ET.Element):
        return cls(class_id=int(node.attrib["class_id"]))

    def evaluate(self, ctx: ImageContextModel, det: DetectionModel) -> bool:
        return det.global_class_id == self.class_id


# ================================================================
# FINAL DATABASE CONDITIONS
# ================================================================

TFDbCond = TypeVar("TFDbCond", bound="FinalDatabaseCondition")


class FinalDatabaseCondition(BaseModel):
    """
    Base para condições da etapa FinalDatabaseRules.
    Avalia cada DetectionModel e decide se ela deve ir para o DB final.
    """
    model_config = {"arbitrary_types_allowed": True}

    _registry: ClassVar[Dict[str, Type["FinalDatabaseCondition"]]] = {}

    @classmethod
    def register_xml_tag(cls, tag: str):
        def deco(sub_cls: Type["FinalDatabaseCondition"]):
            cls._registry[tag] = sub_cls
            return sub_cls
        return deco

    @classmethod
    def from_xml_node(cls: Type[TFDbCond], node: ET.Element) -> TFDbCond:
        tag = node.tag
        if tag not in cls._registry:
            raise ValueError(f"FinalDatabaseCondition XML desconhecida: <{tag}>")
        return cls._registry[tag].from_xml(node)  # type: ignore

    def evaluate(self, ctx: ImageContextModel, det: DetectionModel) -> bool:
        raise NotImplementedError()


@FinalDatabaseCondition.register_xml_tag("DetectionIsNot")
class FDB_DetectionIsNot(FinalDatabaseCondition):
    """
    Condition: a detecção NÃO é de um determinado class_id.
    Só passa se det.global_class_id != class_id.
    """
    class_id: int

    @classmethod
    def from_xml(cls, node: ET.Element):
        return cls(class_id=int(node.attrib["class_id"]))

    def evaluate(self, ctx: ImageContextModel, det: DetectionModel) -> bool:
        return det.global_class_id != self.class_id


@FinalDatabaseCondition.register_xml_tag("or")
class FDB_Or(FinalDatabaseCondition):
    """
    OR de FinalDatabaseConditions.
    Se QUALQUER uma interna for verdadeira para a detecção, retorna True.
    """
    conditions: List[FinalDatabaseCondition]

    @classmethod
    def from_xml(cls, node: ET.Element):
        sub = [FinalDatabaseCondition.from_xml_node(c) for c in node]
        return cls(conditions=sub)

    def evaluate(self, ctx: ImageContextModel, det: DetectionModel) -> bool:
        return any(c.evaluate(ctx, det) for c in self.conditions)


class FinalDatabaseDetectionConditions(BaseModel):
    """
    Bloco <FinalDatabaseDetectionConditions Schema="...">
    No XML novo, Database não existe aqui (assumimos database do root).
    """
    Schema: Optional[str] = None
    conditions: List[FinalDatabaseCondition] = Field(default_factory=list)

    @classmethod
    def from_xml(cls, node: ET.Element):
        schema_name = node.attrib.get("Schema")  # opcional
        conds: List[FinalDatabaseCondition] = []
        for child in node:
            conds.append(FinalDatabaseCondition.from_xml_node(child))
        return cls(Schema=schema_name, conditions=conds)


# ================================================================
# ROUTING RULE
# ================================================================

class RoutingRule(BaseModel):
    network_name: str
    description: Optional[str] = None
    conditions: List[RoutingCondition] = Field(default_factory=list)

    @classmethod
    def from_xml(cls, node: ET.Element):
        network = node.attrib["network_name"]
        desc = node.attrib.get("description")
        conds: List[RoutingCondition] = []

        conds_elem = node.find("conditions")
        if conds_elem is not None:
            for c in conds_elem:
                conds.append(RoutingCondition.from_xml_node(c))

        return cls(network_name=network, description=desc, conditions=conds)

    def matches(self, ctx: ImageContextModel) -> bool:
        return all(c.evaluate(ctx) for c in self.conditions)


# ================================================================
# DETECTION RULE
# ================================================================

class DetectionRule(BaseModel):
    target_task: str
    conditions: List[DetectionCondition] = Field(default_factory=list)

    @classmethod
    def from_xml(cls, node: ET.Element):
        task = node.attrib["target_task"]
        conds: List[DetectionCondition] = []

        conds_elem = node.find("conditions")
        if conds_elem is not None:
            for c in conds_elem:
                conds.append(DetectionCondition.from_xml_node(c))

        return cls(target_task=task, conditions=conds)

    def matches(self, ctx: ImageContextModel, det: DetectionModel) -> bool:
        return all(c.evaluate(ctx, det) for c in self.conditions)


# ================================================================
# FINAL DETECTION ROUTE (SINGULAR)
# ================================================================

class FinalDetectionRoute(BaseModel):
    detection: DetectionModel
    target_tasks: List[str] = Field(default_factory=list)

    def get_first(self) -> Optional[str]:
        return self.target_tasks[0] if self.target_tasks else None

    def get_last(self) -> Optional[str]:
        return self.target_tasks[-1] if self.target_tasks else None

    def get_by_priority(self, priority_index: int) -> Optional[str]:
        raise NotImplementedError("priority system not implemented")


# ================================================================
# FINAL DATABASE ROUTE (MAP POR DATABASE)
# ================================================================

class FinalDatabaseRoute(BaseModel):
    """
    Representa o resultado final para um database específico:
    - database: nome do banco (do root POSTGRES_DATABASE)
    - schema: schema destino (do bloco Schema="...")
    - detections: lista de detecções
    """
    Schema: str
    detections: List[DetectionModel] = Field(default_factory=list)


# ================================================================
# CONFIG DO XML (NOVO FORMATO)
# ================================================================

class RabbitConfig(BaseModel):
    worker_concurrency: int
    rabbit_host: str
    rabbit_port: int
    rabbit_user: str
    rabbit_pass: str
    rabbit_main_queue: str
    rabbit_entry_queue: str


class PostgresConfig(BaseModel):
    postgres_host: str
    postgres_port: int
    postgres_user: str
    postgres_pass: str
    postgres_database: str
    postgres_consolidator_schema: Optional[str] = None


class RoutingsConfig(BaseModel):
    network_queue_prefix: str = "fila_"


class SgcConfig(BaseModel):
    sgc_api_url: str
    sgc_http_timeout_s: int = 30


# ================================================================
# PROCESSING RULES ROOT
# ================================================================

class ProcessingRules(BaseModel):
    xml_path: Path
    version: Optional[str] = None

    rabbit: RabbitConfig
    postgres: PostgresConfig

    routings_config: RoutingsConfig
    routings: List[RoutingRule] = Field(default_factory=list)

    sgc: Optional[SgcConfig] = None
    detection_rules: List[DetectionRule] = Field(default_factory=list)

    final_database_detection_conditions: List[FinalDatabaseDetectionConditions] = Field(default_factory=list)

    model_config = {"arbitrary_types_allowed": True}

    def __init__(__self__, xml_path: Union[str, Path]):
        xml_path = Path(xml_path)
        data = __self__._load(xml_path)

        super().__init__(
            xml_path=xml_path,
            version=data["version"],
            rabbit=data["rabbit"],
            postgres=data["postgres"],
            routings_config=data["routings_config"],
            routings=data["routings"],
            sgc=data["sgc"],
            detection_rules=data["detection_rules"],
            final_database_detection_conditions=data["final_db_blocks"],
        )

    @staticmethod
    def _req_attr(node: ET.Element, name: str) -> str:
        v = node.attrib.get(name)
        if v is None or v == "":
            raise ValueError(f"XML requer atributo '{name}' em <{node.tag}>")
        return v

    @staticmethod
    def _opt_attr(node: ET.Element, name: str, default: Optional[str] = None) -> Optional[str]:
        v = node.attrib.get(name)
        if v is None or v == "":
            return default
        return v

    @staticmethod
    def _load(path: Path) -> Dict[str, Any]:
        tree = ET.parse(path)
        root = tree.getroot()

        if root.tag != "ProcessingRules":
            raise ValueError(f"Root inválido: esperado <ProcessingRules>, veio <{root.tag}>")

        version = root.attrib.get("version")

        rabbit = RabbitConfig(
            worker_concurrency=int(ProcessingRules._req_attr(root, "WORKER_CONCURRENCY")),
            rabbit_host=ProcessingRules._req_attr(root, "RABBIT_HOST"),
            rabbit_port=int(ProcessingRules._req_attr(root, "RABBIT_PORT")),
            rabbit_user=ProcessingRules._req_attr(root, "RABBIT_USER"),
            rabbit_pass=ProcessingRules._req_attr(root, "RABBIT_PASS"),
            rabbit_main_queue=ProcessingRules._req_attr(root, "RABBIT_MAIN_QUEUE"),
            rabbit_entry_queue=ProcessingRules._req_attr(root, "RABBIT_ENTRY_QUEUE"),
        )

        postgres = PostgresConfig(
            postgres_host=ProcessingRules._req_attr(root, "POSTGRES_HOST"),
            postgres_port=int(ProcessingRules._req_attr(root, "POSTGRES_PORT")),
            postgres_user=ProcessingRules._req_attr(root, "POSTGRES_USER"),
            postgres_pass=ProcessingRules._req_attr(root, "POSTGRES_PASS"),
            postgres_database=ProcessingRules._req_attr(root, "POSTGRES_DATABASE"),
            postgres_consolidator_schema=ProcessingRules._opt_attr(root, "POSTGRES_CONSOLIDATOR_SCHEMA", None),
        )

        routings_config = RoutingsConfig(network_queue_prefix="fila_")
        routings: List[RoutingRule] = []
        routings_elem = root.find("routings")
        if routings_elem is not None:
            routings_config = RoutingsConfig(
                network_queue_prefix=ProcessingRules._opt_attr(routings_elem, "NETWORK_QUEUE_PREFIX", "fila_") or "fila_"
            )
            for r in routings_elem.findall("routing"):
                routings.append(RoutingRule.from_xml(r))

        sgc: Optional[SgcConfig] = None
        detection_rules: List[DetectionRule] = []
        det_rules_elem = root.find("DetectionsRules")
        if det_rules_elem is not None:
            sgc = SgcConfig(
                sgc_api_url=ProcessingRules._req_attr(det_rules_elem, "SGC_API_URL"),
                sgc_http_timeout_s=int(ProcessingRules._opt_attr(det_rules_elem, "SGC_HTTP_TIMEOUT_S", "30") or "30"),
            )
            for d in det_rules_elem.findall("DetectionRule"):
                detection_rules.append(DetectionRule.from_xml(d))

        final_db_blocks: List[FinalDatabaseDetectionConditions] = []
        final_elem = root.find("FinalDatabaseRules")
        if final_elem is not None:
            for block in final_elem.findall("FinalDatabaseDetectionConditions"):
                final_db_blocks.append(FinalDatabaseDetectionConditions.from_xml(block))

        return {
            "version": version,
            "rabbit": rabbit,
            "postgres": postgres,
            "routings_config": routings_config,
            "routings": routings,
            "sgc": sgc,
            "detection_rules": detection_rules,
            "final_db_blocks": final_db_blocks,
        }

    # -------------------------------------------------------------
    # ROUTING (REDES)
    # -------------------------------------------------------------
    def get_next_network_routes(self, ctx: ImageContextModel = None) -> List[str]:
        """
        Retorna a lista de redes que ainda devem ser processadas.
        """
        ctx = ctx or ImageContextModel.get_empty_context()

        return [r.network_name for r in self.routings if r.matches(ctx)]

    # -------------------------------------------------------------
    # DETECTION ROUTING (tasks)
    # -------------------------------------------------------------
    def get_final_detections_routes(self, ctx: ImageContextModel) -> List[FinalDetectionRoute]:
        """
        Uma rota por detecção (todas as detecções de todos os jobs),
        contendo as target_tasks aplicáveis.
        """
        final_routes: List[FinalDetectionRoute] = []

        for det in iter_detections(ctx):
            tasks: List[str] = []
            for rule in self.detection_rules:
                if rule.matches(ctx, det):
                    tasks.append(rule.target_task)

            final_routes.append(FinalDetectionRoute(detection=det, target_tasks=tasks))

        return final_routes

    # -------------------------------------------------------------
    # FINAL DATABASE (map por database)
    # -------------------------------------------------------------
    def get_final_database_detections(
        self,
        ctx: ImageContextModel,
        finalDetectionsRouteMap: List[FinalDetectionRoute],
    ) -> List[FinalDatabaseRoute]:
        """
        Retorna uma lista de FinalDatabaseRoute.
        Cada FinalDatabaseRoute representa um database/schema e contém as
        detecções que:
          - NÃO foram enviadas para o SGC (target_tasks vazio no FinalDetectionRoute)
          - E passam por TODAS as FinalDatabaseConditions do bloco correspondente.
        """
        if not self.final_database_detection_conditions:
            return []

        candidate_detections: List[DetectionModel] = [
            route.detection
            for route in finalDetectionsRouteMap
            if not route.target_tasks
        ]
        if not candidate_detections:
            return []

        db_routes: List[FinalDatabaseRoute] = []

        for db_block in self.final_database_detection_conditions:
            if not db_block.conditions:
                db_dets = candidate_detections.copy()
            else:
                db_dets = [det for det in candidate_detections if all(cond.evaluate(ctx, det) for cond in db_block.conditions)]

            if db_dets:
                db_routes.append(
                    FinalDatabaseRoute(
                        Schema=db_block.Schema,
                        detections=db_dets,
                    )
                )

        return db_routes
