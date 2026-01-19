from __future__ import annotations
from datetime import datetime
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional, Type, TypeVar, ClassVar, Union, Any

from pydantic import BaseModel, Field

# ================================================================
# IMAGE CONTEXT MODELS
# ================================================================

class ImageData(BaseModel):
    image_id: int
    image_path: Optional[str] = None
    ibge_code: Optional[int] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    created_at: Optional[datetime] = None
    inserted_at: datetime


class Detection(BaseModel):
    detection_id: int
    image_id: int
    global_class_id: Optional[int] = None
    network_name: Optional[str] = None
    confidence: Optional[float] = None
    x: Optional[float] = None
    y: Optional[float] = None
    width: Optional[float] = None
    height: Optional[float] = None
    mask: Optional[Any] = None
    created_at: datetime


class NetworkStatus(BaseModel):
    net_status_id: int
    image_id: int
    network_name: Optional[str] = None
    net_status: str
    updated_at: datetime


class ImageContext(BaseModel):
    image: Optional[ImageData] = None
    detections: List[Detection] = Field(default_factory=list)
    network_status: List[NetworkStatus] = Field(default_factory=list)


# ================================================================
# ROUTING CONDITIONS
# ================================================================

TRoutingCond = TypeVar("TRoutingCond", bound="RoutingCondition")

class RoutingCondition(BaseModel):
    """
    Base para condições usadas pelas regras de roteamento de redes.
    Avalia apenas com base no ImageContext.
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

    def evaluate(self, ctx: ImageContext) -> bool:
        raise NotImplementedError()


@RoutingCondition.register_xml_tag("NotProcessedInNetwork")
class RC_NotProcessedInNetwork(RoutingCondition):
    network_name: str

    @classmethod
    def from_xml(cls, node: ET.Element):
        return cls(network_name=node.attrib["network_name"])

    def evaluate(self, ctx: ImageContext) -> bool:
        return not any(
            ns.network_name == self.network_name and ns.net_status in ("pending", "done")
            for ns in ctx.network_status
        )


@RoutingCondition.register_xml_tag("ProcessingFinishedInNetwork")
class RC_ProcessingFinishedInNetwork(RoutingCondition):
    network_name: str

    @classmethod
    def from_xml(cls, node: ET.Element):
        return cls(network_name=node.attrib["network_name"])

    def evaluate(self, ctx: ImageContext) -> bool:
        return any(
            ns.network_name == self.network_name and ns.net_status == "done"
            for ns in ctx.network_status
        )


@RoutingCondition.register_xml_tag("RequiresDetectionOfClass")
class RC_RequiresDetectionOfClass(RoutingCondition):
    class_id: int

    @classmethod
    def from_xml(cls, node: ET.Element):
        return cls(class_id=int(node.attrib["class_id"]))

    def evaluate(self, ctx: ImageContext) -> bool:
        return any(det.global_class_id == self.class_id for det in ctx.detections)


@RoutingCondition.register_xml_tag("CityIsIbge")
class RC_CityIsIbge(RoutingCondition):
    code: int

    @classmethod
    def from_xml(cls, node: ET.Element):
        return cls(code=int(node.attrib["code"]))

    def evaluate(self, ctx: ImageContext) -> bool:
        return ctx.image is not None and ctx.image.ibge_code == self.code


@RoutingCondition.register_xml_tag("or")
class RC_Or(RoutingCondition):
    conditions: List[RoutingCondition]

    @classmethod
    def from_xml(cls, node: ET.Element):
        sub = [RoutingCondition.from_xml_node(c) for c in node]
        return cls(conditions=sub)

    def evaluate(self, ctx: ImageContext) -> bool:
        return any(c.evaluate(ctx) for c in self.conditions)


# ================================================================
# DETECTION CONDITIONS
# ================================================================

TDetCond = TypeVar("TDetCond", bound="DetectionCondition")

class DetectionCondition(BaseModel):
    """
    Base para condições usadas pelas regras de detecção (DetectionsRules).
    Avalia com base no ImageContext E na Detection.
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

    def evaluate(self, ctx: ImageContext, det: Detection) -> bool:
        raise NotImplementedError()


@DetectionCondition.register_xml_tag("DetectionIs")
class DC_DetectionIs(DetectionCondition):
    class_id: int

    @classmethod
    def from_xml(cls, node: ET.Element):
        return cls(class_id=int(node.attrib["class_id"]))

    def evaluate(self, ctx: ImageContext, det: Detection) -> bool:
        return det.global_class_id == self.class_id


# ================================================================
# FINAL DATABASE CONDITIONS
# ================================================================

TFDbCond = TypeVar("TFDbCond", bound="FinalDatabaseCondition")

class FinalDatabaseCondition(BaseModel):
    """
    Base para condições da etapa FinalDatabaseRules.
    Avalia cada Detection e decide se ela deve ir para o DB final.
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

    def evaluate(self, ctx: ImageContext, det: Detection) -> bool:
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

    def evaluate(self, ctx: ImageContext, det: Detection) -> bool:
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

    def evaluate(self, ctx: ImageContext, det: Detection) -> bool:
        return any(c.evaluate(ctx, det) for c in self.conditions)

class FinalDatabaseDetectionConditions(BaseModel):
    """
    Bloco <FinalDatabaseDetectionConditions Database="..." Schema="...">
    Agrupa as FinalDatabaseConditions e guarda o nome do database e schema finais.
    """
    database: str
    schema: Optional[str] = None
    conditions: List[FinalDatabaseCondition] = Field(default_factory=list)

    @classmethod
    def from_xml(cls, node: ET.Element):
        db_name = node.attrib.get("Database")
        if not db_name:
            raise ValueError("<FinalDatabaseDetectionConditions> requer atributo Database")

        schema_name = node.attrib.get("Schema")  # opcional

        conds: List[FinalDatabaseCondition] = []
        for child in node:
            conds.append(FinalDatabaseCondition.from_xml_node(child))

        return cls(
            database=db_name,
            schema=schema_name,
            conditions=conds,
        )


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
        conds = []

        conds_elem = node.find("conditions")
        if conds_elem:
            for c in conds_elem:
                conds.append(RoutingCondition.from_xml_node(c))

        return cls(network_name=network, description=desc, conditions=conds)

    def matches(self, ctx: ImageContext) -> bool:
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
        conds = []

        conds_elem = node.find("conditions")
        if conds_elem:
            for c in conds_elem:
                conds.append(DetectionCondition.from_xml_node(c))

        return cls(target_task=task, conditions=conds)

    def matches(self, ctx: ImageContext, det: Detection) -> bool:
        return all(c.evaluate(ctx, det) for c in self.conditions)


# ================================================================
# FINAL DETECTION ROUTE (SINGULAR)
# ================================================================

class FinalDetectionRoute(BaseModel):
    detection: Detection
    target_tasks: List[str] = Field(default_factory=list)

    def get_first(self) -> Optional[str]:
        return self.target_tasks[0] if self.target_tasks else None

    def get_last(self) -> Optional[str]:
        return self.target_tasks[-1] if self.target_tasks else None

    def get_by_priority(self, priority_index: int) -> Optional[str]:
        # if 0 <= priority_index < len(self.target_tasks):
        #     return self.target_tasks[priority_index]
        raise NotImplementedError("priority system not implemented")


# ================================================================
# FINAL DATABASE ROUTE (MAP POR DATABASE)
# ================================================================

class FinalDatabaseRoute(BaseModel):
    """
    Representa o resultado final para um database específico:
    - database: nome do banco (ex: 'renan')
    - schema: schema destino (ex: 'consolidator'), opcional
    - detections: lista de detecções que devem ser enviadas para esse banco/schema
    """
    database: str
    schema: Optional[str] = None
    detections: List[Detection] = Field(default_factory=list)



# ================================================================
# PROCESSING RULES ROOT
# ================================================================

class ProcessingRules(BaseModel):
    xml_path: Path
    version: Optional[str]
    routings: List[RoutingRule]
    detection_rules: List[DetectionRule]
    final_database_detection_conditions: List[FinalDatabaseDetectionConditions]

    model_config = {"arbitrary_types_allowed": True}

    def __init__(__self__, xml_path: Union[str, Path]):
        xml_path = Path(xml_path)
        (
            version,
            routings,
            detection_rules,
            final_db_blocks,
        ) = __self__._load(xml_path)

        super().__init__(
            xml_path=xml_path,
            version=version,
            routings=routings,
            detection_rules=detection_rules,
            final_database_detection_conditions=final_db_blocks,
        )

    @staticmethod
    def _load(path: Path):
        tree = ET.parse(path)
        root = tree.getroot()

        version = root.attrib.get("version")

        # Routing rules
        rrules: List[RoutingRule] = []
        relem = root.find("routings")
        if relem:
            for r in relem.findall("routing"):
                rrules.append(RoutingRule.from_xml(r))

        # Detection rules
        drules: List[DetectionRule] = []
        delem = root.find("DetectionsRules")
        if delem:
            for d in delem.findall("DetectionRule"):
                drules.append(DetectionRule.from_xml(d))

        # Final database detection conditions (podem existir vários blocos)
        final_db_blocks: List[FinalDatabaseDetectionConditions] = []
        felem = root.find("FinalDatabaseRules")
        if felem is not None:
            for conds_elem in felem.findall("FinalDatabaseDetectionConditions"):
                final_db_blocks.append(FinalDatabaseDetectionConditions.from_xml(conds_elem))

        return version, rrules, drules, final_db_blocks

    # -------------------------------------------------------------
    # ROUTING (REDES)
    # -------------------------------------------------------------
    def get_next_network_routes(self, ctx: ImageContext) -> List[str]:
        """
        Retorna a lista de redes que ainda devem ser processadas
        com base nas regras de roteamento e no estado atual do contexto.
        """
        return [r.network_name for r in self.routings if r.matches(ctx)]

    # -------------------------------------------------------------
    # DETECTION ROUTING (tasks)
    # -------------------------------------------------------------
    def get_final_detections_routes(self, ctx: ImageContext) -> List[FinalDetectionRoute]:
        """
        Retorna uma lista pura de FinalDetectionRoute,
        uma para cada detecção, contendo as target_tasks aplicáveis.
        """
        finalDetectionsRoutesMap: List[FinalDetectionRoute] = []

        for det in ctx.detections:
            tasks: List[str] = []
            for rule in self.detection_rules:
                if rule.matches(ctx, det):
                    tasks.append(rule.target_task)

            finalDetectionsRoutesMap.append(
                FinalDetectionRoute(
                    detection=det,
                    target_tasks=tasks,
                )
            )

        return finalDetectionsRoutesMap

    # -------------------------------------------------------------
    # FINAL DATABASE (map por database)
    # -------------------------------------------------------------
    def get_final_database_detections_map(
        self,
        ctx: ImageContext,
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

        candidate_detections: List[Detection] = [
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
                db_dets: List[Detection] = []
                for det in candidate_detections:
                    if all(cond.evaluate(ctx, det) for cond in db_block.conditions):
                        db_dets.append(det)

            if db_dets:
                db_routes.append(
                    FinalDatabaseRoute(
                        database=db_block.database,
                        schema=db_block.schema,  # <<---- AQUI
                        detections=db_dets,
                    )
                )

        return db_routes

