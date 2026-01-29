# postgres_repository.py  (schema consolidator + jetson_detections no image_data)
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, ClassVar, Dict, Iterable, List, Optional, Tuple

import asyncpg
from pydantic import BaseModel, ConfigDict, Field

from src.messages import ConsolidatorEntryMessage, YoloDetection
from typing import Any, Dict, List, Optional
import json

# ============================================================
# Pydantic models (alinhados ao schema consolidator)
# ============================================================

class ImageDataModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int
    external_image_id: int
    image_path: str

    device: str
    device_id: str
    camera_id: str
    ibge_code: str

    latitude: float
    longitude: float
    jetson_timestamp: datetime

    jetson_detections: List[Dict[str, Any]] = Field(default_factory=list)

    inserted_at: datetime
    created_at: datetime


class JobModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    FINISHED: ClassVar[str] = "finished"
    PROCESSING: ClassVar[str] = "processing"

    job_id: int
    status: str

    image_pk: int

    network_name: str
    network_version: str

    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    stored_at: Optional[datetime] = None

    detections: List[YoloDetection] = Field(default_factory=list)


class ImageContextModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    image: Optional[ImageDataModel] = None
    jobs: List[JobModel] = Field(default_factory=list)

    @staticmethod
    def get_empty_context() -> "ImageContextModel":
        return ImageContextModel()

    def has_processing_jobs(self) -> bool:
        return any(job.status == JobModel.PROCESSING for job in self.jobs)


# ============================================================
# Repository
# ============================================================

class PostgresRepository:
    def __init__(self, pool: asyncpg.Pool, *, schema: str) -> None:
        self.pool = pool
        self._schema = schema

    # ------------------------------------------------------------
    # IMAGE: cria apenas se não existir
    # Retorna: (image_pk, inserted_new)
    # ------------------------------------------------------------
    async def create_image_data_from_entry(
        self,
        *,
        payload: ConsolidatorEntryMessage,
        conn: asyncpg.Connection,
    ) -> Tuple[int, bool]:
        s = self._schema

        external_image_id = int(payload.image_id)
        image_path = str(payload.image_path)

        device = str(payload.tags.device)
        device_id = str(payload.tags.device_id)
        camera_id = str(payload.tags.camera_id)
        ibge_code = str(payload.tags.ibge_code)

        latitude = float(payload.jetson_data.latitude)
        longitude = float(payload.jetson_data.longitude)
        jetson_timestamp = payload.jetson_data.timestamp

        jetson_detections_list = [d.model_dump() for d in payload.jetson_data.detections]
        jetson_detections_json = json.dumps(jetson_detections_list)


        row = await conn.fetchrow(
            f"""
            INSERT INTO {s}.image_data (
                external_image_id,
                image_path,
                device, device_id, camera_id, ibge_code,
                latitude, longitude, jetson_timestamp,
                jetson_detections
            )
            VALUES (
                $1, $2,
                $3, $4, $5, $6,
                $7, $8, $9,
                $10::jsonb
            )
            ON CONFLICT DO NOTHING
            RETURNING id
            """,
            external_image_id,
            image_path,
            device, device_id, camera_id, ibge_code,
            latitude, longitude, jetson_timestamp,
            jetson_detections_json,
        )

        if row:
            return int(row["id"]), True

        row2 = await conn.fetchrow(
            f"SELECT id FROM {s}.image_data WHERE external_image_id = $1",
            external_image_id,
        )
        if row2:
            return int(row2["id"]), False

        row3 = await conn.fetchrow(
            f"SELECT id FROM {s}.image_data WHERE image_path = $1",
            image_path,
        )
        if not row3:
            raise RuntimeError(
                "Conflitou no INSERT (ON CONFLICT DO NOTHING), mas não achei a linha por external_image_id nem image_path."
            )
        return int(row3["id"]), False

    # ------------------------------------------------------------
    # JOB: cria/pega job, mas NÃO seta started_at aqui
    # - created_at é setado pelo DEFAULT do banco
    # ------------------------------------------------------------
    async def start_job(
        self,
        *,
        image_pk: int,
        network_name: str,
        network_version: str = "1",
        conn: asyncpg.Connection,
    ) -> int:
        s = self._schema


        img = await conn.fetchrow(
            f"SELECT id FROM {s}.image_data WHERE id=$1 FOR UPDATE",
            image_pk,
        )
        if not img:
            raise LookupError(f"image_pk={image_pk} não existe em {s}.image_data")

        # NÃO passa started_at; deixa NULL
        row = await conn.fetchrow(
            f"""
            INSERT INTO {s}.job (status, image_pk, network_name, network_version)
            VALUES ('processing', $1, $2, $3)
            ON CONFLICT (image_pk, network_name, network_version)
            DO UPDATE SET
              status = 'processing'
            RETURNING job_id
            """,
            image_pk,
            network_name,
            network_version,
        )
        if not row:
            raise RuntimeError("Falha ao criar/obter job_id.")
        return int(row["job_id"])

    # ------------------------------------------------------------
    # JOB: finaliza o job setando started_at e finished_at (ambos vêm por parâmetro)
    # - também insere detections do YOLO
    # ------------------------------------------------------------
    async def finish_job(
        self,
        *,
        job_id: int,
        started_at: datetime,
        finished_at: datetime,
        detections: Optional[Iterable[YoloDetection]] = None,
        conn: asyncpg.Connection,
    ) -> int:
        """
        Marca o job como finished (seta started_at + finished_at),
        insere as detecções geradas por ele e retorna o image_pk.
        """
        s = self._schema


        # 1) Atualiza job e pega image_pk
        row = await conn.fetchrow(
            f"""
            UPDATE {s}.job
            SET status = 'finished',
                started_at = $2,
                finished_at = $3
            WHERE job_id = $1
            RETURNING image_pk
            """,
            job_id,
            started_at,
            finished_at,
        )
        if not row:
            raise LookupError(f"job_id={job_id} não existe em {s}.job")

        image_pk = int(row["image_pk"])

        # 2) Insere detecções (se houver)
        if detections:
            insert_sql = f"""
                INSERT INTO {s}.detections (
                    job_id,
                    label, id, confidence,
                    x, y, width, height,
                    image_shape,
                    mask
                )
                VALUES (
                    $1,
                    $2, $3, $4,
                    $5, $6, $7, $8,
                    $9::int[],
                    $10::jsonb
                )
            """

            for det in detections:
                image_shape = [int(det.image_shape[0]), int(det.image_shape[1])]
                # garante jsonb list
                mask_json = json.dumps(det.mask) if det.mask is not None else "[]"

                await conn.execute(
                    insert_sql,
                    job_id,
                    str(det.label),
                    int(det.id),
                    float(det.confidence),
                    float(det.x),
                    float(det.y),
                    float(det.width),
                    float(det.height),
                    image_shape,
                    mask_json,
                )

        return image_pk

    # ------------------------------------------------------------
    # CONTEXT: busca por image_pk (id interno)
    # ------------------------------------------------------------



    def _coerce_jsonb_list(self,value: Any) -> List[Dict[str, Any]]:
        if value is None:
            return []
        if isinstance(value, list):
            # ideal: driver já decodou o jsonb
            return value
        if isinstance(value, str):
            # veio json como texto
            parsed = json.loads(value)
            if not isinstance(parsed, list):
                raise TypeError(f"jetson_detections JSON não é lista: {type(parsed)}")
            # garante dicts
            out: List[Dict[str, Any]] = []
            for item in parsed:
                if not isinstance(item, dict):
                    raise TypeError(f"item em jetson_detections não é dict: {type(item)}")
                out.append(item)
            return out
        # fallback (ex: asyncpg Record/Json etc)
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
        raise TypeError(f"Tipo inesperado em jetson_detections: {type(value)}")


    async def get_image_context_by_image_pk(self,*, image_pk: int,conn: asyncpg.Connection) -> ImageContextModel:
        s = self._schema


        img_row = await conn.fetchrow(
            f"""
            SELECT
              id,
              external_image_id,
              image_path,
              device, device_id, camera_id, ibge_code,
              latitude, longitude, jetson_timestamp,
              jetson_detections,
              inserted_at,
              created_at
            FROM {s}.image_data
            WHERE id = $1
            """,
            image_pk,
        )
        if not img_row:
            raise LookupError(f"image_pk={image_pk} não existe em {s}.image_data")

        print("img_row",img_row)
        #jetson_detections = '[{"shape": "0.277734,0.884896,0.199219,0.084375", "class_id": 26, "confidence": 0.400292, "shape_type": "bounding_box"}]'
        image = ImageDataModel(
            id=int(img_row["id"]),
            external_image_id=int(img_row["external_image_id"]),
            image_path=str(img_row["image_path"]),
            device=str(img_row["device"]),
            device_id=str(img_row["device_id"]),
            camera_id=str(img_row["camera_id"]),
            ibge_code=str(img_row["ibge_code"]),
            latitude=float(img_row["latitude"]),
            longitude=float(img_row["longitude"]),
            jetson_timestamp=img_row["jetson_timestamp"],
            jetson_detections= self._coerce_jsonb_list(img_row["jetson_detections"]),
            inserted_at=img_row["inserted_at"],
            created_at=img_row["created_at"],
        )

        job_rows = await conn.fetch(
            f"""
            SELECT
              j.job_id,
              j.status,
              j.image_pk,
              j.network_name,
              j.network_version,
              j.created_at,
              j.started_at,
              j.finished_at,
              j.stored_at
            FROM {s}.job j
            WHERE j.image_pk = $1
            ORDER BY j.created_at ASC, j.job_id ASC
            """,
            image_pk,
        )

        jobs_by_id: Dict[int, JobModel] = {}
        for r in job_rows:
            jid = int(r["job_id"])
            jobs_by_id[jid] = JobModel(
                job_id=jid,
                status=str(r["status"]),
                image_pk=int(r["image_pk"]),
                network_name=str(r["network_name"]),
                network_version=str(r["network_version"]),
                created_at=r["created_at"],
                started_at=r["started_at"],
                finished_at=r["finished_at"],
                stored_at=r["stored_at"],
                detections=[],
            )

        if not jobs_by_id:
            return ImageContextModel(image=image, jobs=[])

        job_ids = list(jobs_by_id.keys())

        det_rows = await conn.fetch(
            f"""
            SELECT
              d.job_id,
              d.label, d.id, d.confidence,
              d.x, d.y, d.width, d.height,
              d.image_shape,
              d.mask
            FROM {s}.detections d
            WHERE d.job_id = ANY($1::bigint[])
            ORDER BY d.detection_id ASC
            """,
            job_ids,
        )

        for r in det_rows:
            det = YoloDetection(
                label=str(r["label"]),
                id=int(r["id"]),
                confidence=float(r["confidence"]),
                x=float(r["x"]),
                y=float(r["y"]),
                width=float(r["width"]),
                height=float(r["height"]),
                image_shape=list(r["image_shape"] or []),
                mask=list(r["mask"]) if r["mask"] is not None else [],
            )
            jobs_by_id[int(r["job_id"])].detections.append(det)

        return ImageContextModel(image=image, jobs=list(jobs_by_id.values()))

    async def insert_detections_in_final_database(
        self,
        *,
        detections: list[YoloDetection],
        schema: str,
        context: ImageContextModel,
        conn: asyncpg.Connection,
    ) -> int:
        """
        Insere detections no schema final (ex: ia_zeladoria).

        Regras:
        - Usa context.image.external_image_id para achar (ou criar) ia_zeladoria.image_data e obter image_id
        - Usa detection.id como global_class_id
        - Insere em ia_zeladoria.detections:
            (image_id, global_class_id, confidence, x, y, width, height, mask)
        - ON CONFLICT (image_id, global_class_id, x, y, width, height) DO NOTHING
        - Retorna quantidade de inserts efetivos
        """
        if not context.image:
            raise ValueError("context.image está vazio; não consigo obter external_image_id.")

        s = schema
        external_image_id = int(context.image.external_image_id)

        if not detections:
            return 0

        # 1) garantir/obter image_id no schema final
        # (assumindo que image_path é NOT NULL no schema final)
        row = await conn.fetchrow(
            f"""
            SELECT image_id
            FROM {s}.image_data
            WHERE (tags->>'external_image_id')::bigint = $1
            """,
            external_image_id,
        )

        if row:
            image_id = int(row["image_id"])
        else:
            # cria uma linha mínima na image_data final
            # - guarda external_image_id dentro de tags
            # - usa o image_path do consolidator como image_path final
            tags_json = json.dumps(
                {
                    "external_image_id": external_image_id,
                    "device": context.image.device,
                    "device_id": context.image.device_id,
                    "camera_id": context.image.camera_id,
                    "ibge_code": context.image.ibge_code,
                    "jetson_timestamp": context.image.jetson_timestamp.isoformat(),
                }
            )

            inserted = await conn.fetchrow(
                f"""
                INSERT INTO {s}.image_data (image_path, tags, geom)
                VALUES (
                    $1,
                    $2::jsonb,
                    ST_SetSRID(ST_MakePoint($3, $4), 4326)
                )
                RETURNING image_id
                """,
                context.image.image_path,
                tags_json,
                float(context.image.longitude),
                float(context.image.latitude),
            )
            image_id = int(inserted["image_id"])

        # 2) inserir detections
        insert_sql = f"""
            INSERT INTO {s}.detections (
                image_id,
                global_class_id,
                confidence,
                x, y, width, height,
                mask
            )
            VALUES (
                $1,
                $2,
                $3,
                $4, $5, $6, $7,
                $8::jsonb
            )
            ON CONFLICT (image_id, global_class_id, x, y, width, height)
            DO NOTHING
        """

        inserted_count = 0

        for det in detections:
            mask_json = json.dumps(det.mask) if det.mask is not None else "[]"

            status = await conn.execute(
                insert_sql,
                image_id,
                int(det.id),                 # global_class_id
                float(det.confidence),
                float(det.x),
                float(det.y),
                float(det.width),
                float(det.height),
                mask_json,
            )
            # asyncpg retorna "INSERT 0 1" ou "INSERT 0 0"
            if status.endswith(" 1"):
                inserted_count += 1

        return inserted_count

    async def init_lock_in_image_data(self, job_id: int, conn: asyncpg.Connection) -> int:
        """
        Pega job.image_pk e faz SELECT ... FOR UPDATE na consolidator.image_data(id=image_pk).

        - Fica aguardando o lock (bloqueado) até 30s.
        - Se não conseguir em 30s, levanta exceção.
        - Pressupõe que você já está dentro de uma transaction (conn.transaction()).
        """
        # 1) Descobre o image_pk do job
        row = await conn.fetchrow(
            """
            SELECT image_pk
            FROM consolidator.job
            WHERE job_id = $1
            """,
            job_id,
        )
        if not row:
            raise LookupError(f"job_id={job_id} não existe em consolidator.job")

        image_pk = int(row["image_pk"])

        # 2) Define timeout de lock só pra esta transaction (ou subtransaction)
        #    (SET LOCAL só funciona dentro de transaction)
        await conn.execute("SET LOCAL lock_timeout = '30s'")

        # 3) Tenta adquirir o lock da linha (vai aguardar até 30s)
        try:
            locked = await conn.fetchrow(
                """
                SELECT id
                FROM consolidator.image_data
                WHERE id = $1
                    FOR UPDATE
                """,
                image_pk,
            )
        except asyncpg.PostgresError as e:
            # lock_timeout normalmente vem como "canceling statement due to lock timeout"
            # sqlstate costuma ser 55P03 (lock_not_available) ou, em alguns casos, 57014 (query_canceled)
            if getattr(e, "sqlstate", None) in ("55P03", "57014"):
                raise TimeoutError(
                    f"Timeout (30s) aguardando lock em consolidator.image_data.id={image_pk} "
                    f"(job_id={job_id})"
                ) from e
            raise

        if not locked:
            raise LookupError(f"image_pk={image_pk} (do job_id={job_id}) não existe em consolidator.image_data")

        # Se chegou aqui, a linha está travada (FOR UPDATE) até o fim da transaction.
        return image_pk