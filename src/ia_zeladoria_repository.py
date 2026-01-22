# ia_zeladoria_repository.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, ClassVar

import asyncpg
from pydantic import BaseModel, ConfigDict, Field


# ============================================================
# Pydantic models
# ============================================================

class ImageDataModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    image_id: int
    image_url: str
    tags: Optional[Dict[str, Any]] = None
    inserted_at: datetime
    created_at: datetime
    geom_wkt: Optional[str] = None


class DetectionModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    pass_detections_id: int
    job_id: int
    global_class_id: int
    label:str

    confidence: Optional[float] = None
    x: float
    y: float
    width: float
    height: float

    mask: Optional[Dict[str, Any]] = None # ou Optional[Any]
    created_at: datetime
    sgc_approved: bool = False


class JobModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    FINISHED: ClassVar[str] = "finished"
    PROCESSING: ClassVar[str] = "processing"

    job_id: int
    status: str
    image_id: int

    network_id: int
    network_name: str
    network_version: str

    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    stored_at: Optional[datetime] = None

    detections: List[DetectionModel] = Field(default_factory=list)


class ImageContextModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    image: ImageDataModel = None
    jobs: List[JobModel] = Field(default_factory=list)

    @staticmethod
    def get_empty_context():
        return ImageContextModel()

    def has_processing_jobs(self) -> bool:
        for job in self.jobs:
            if job.status == JobModel.PROCESSING:
                return True
        return False

# ============================================================
# Repository
# ============================================================

class PostgresRepository:
    def __init__(self, pool: asyncpg.Pool, *, schema: str) -> None:
        self._pool = pool
        self._schema = schema

    async def _get_or_create_network_id(
        self,
        conn: asyncpg.Connection,
        *,
        network_name: str,
        network_version: str = "1",
        description: Optional[str] = None,
    ) -> int:
        s = self._schema

        row = await conn.fetchrow(
            f"""
            SELECT network_id
            FROM {s}.network
            WHERE network_name = $1 AND network_version = $2
            """,
            network_name,
            network_version,
        )
        if row:
            return int(row["network_id"])

        row2 = await conn.fetchrow(
            f"""
            INSERT INTO {s}.network (network_name, network_version, description)
            VALUES ($1, $2, $3)
            ON CONFLICT (network_name, network_version) DO UPDATE
              SET description = COALESCE(EXCLUDED.description, {s}.network.description)
            RETURNING network_id
            """,
            network_name,
            network_version,
            description,
        )
        if not row2:
            raise RuntimeError("Falha ao criar/obter network_id.")
        return int(row2["network_id"])

    async def get_image_context_by_image_id(self, image_id: int) -> ImageContextModel:
        s = self._schema

        async with self._pool.acquire() as conn:
            img_row = await conn.fetchrow(
                f"""
                SELECT
                  image_id,
                  image_url,
                  tags,
                  inserted_at,
                  created_at,
                  ST_AsText(geom) AS geom_wkt
                FROM {s}.image_data
                WHERE image_id = $1
                """,
                image_id,
            )
            if not img_row:
                raise LookupError(f"image_id={image_id} não existe em {s}.image_data")

            image = ImageDataModel(
                image_id=int(img_row["image_id"]),
                image_url=str(img_row["image_url"]),
                tags=img_row["tags"],
                inserted_at=img_row["inserted_at"],
                created_at=img_row["created_at"],
                geom_wkt=img_row["geom_wkt"],
            )

            job_rows = await conn.fetch(
                f"""
                SELECT
                  j.job_id,
                  j.status,
                  j.image_id,
                  j.network_id,
                  j.created_at,
                  j.started_at,
                  j.finished_at,
                  j.stored_at,
                  n.network_name,
                  n.network_version
                FROM {s}.job j
                INNER JOIN {s}.network n
                  ON n.network_id = j.network_id
                WHERE j.image_id = $1
                ORDER BY j.created_at ASC, j.job_id ASC
                """,
                image_id,
            )

            jobs_by_id: Dict[int, JobModel] = {}
            for r in job_rows:
                jid = int(r["job_id"])
                jobs_by_id[jid] = JobModel(
                    job_id=jid,
                    status=str(r["status"]),
                    image_id=int(r["image_id"]),
                    network_id=int(r["network_id"]),
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
                  d.pass_detections_id,
                  d.job_id,
                  d.global_class_id,
                  d.confidence,
                  d.x,
                  d.y,
                  d.width,
                  d.height,
                  d.mask,
                  d.created_at,
                  d.sgc_approved
                FROM {s}.detections d
                WHERE d.job_id = ANY($1::bigint[])
                ORDER BY d.created_at ASC, d.pass_detections_id ASC
                """,
                job_ids,
            )

            for r in det_rows:
                det = DetectionModel(
                    pass_detections_id=int(r["pass_detections_id"]),
                    job_id=int(r["job_id"]),
                    global_class_id=int(r["global_class_id"]),
                    confidence=float(r["confidence"]) if r["confidence"] is not None else None,
                    x=float(r["x"]),
                    y=float(r["y"]),
                    width=float(r["width"]),
                    height=float(r["height"]),
                    mask=r["mask"],
                    created_at=r["created_at"],
                    sgc_approved=bool(r["sgc_approved"]),
                )
                jobs_by_id[det.job_id].detections.append(det)

            return ImageContextModel(image=image, jobs=list(jobs_by_id.values()))


    #todo: fazer a trasanction englobar o envio para o rabbit para dar rollback se der erro no envio do rabbit
    async def start_job(
        self,
        *,
        image_id: int,
        network_name: str,
        network_version: str,
    ) -> int:
        s = self._schema
        started_at = datetime.now()

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                img = await conn.fetchrow(
                    f"SELECT image_id FROM {s}.image_data WHERE image_id=$1 FOR UPDATE",
                    image_id,
                )
                if not img:
                    raise LookupError(f"image_id={image_id} não existe em {s}.image_data")

                network_id = await self._get_or_create_network_id(
                    conn,
                    network_name=network_name,
                    network_version=network_version,
                )

                row = await conn.fetchrow(
                    f"""
                    INSERT INTO {s}.job (status, image_id, network_id, started_at)
                    VALUES ('processing', $1, $2, $3)
                    ON CONFLICT (image_id, network_id)
                    DO UPDATE SET
                      started_at = COALESCE({s}.job.started_at, EXCLUDED.started_at)
                    RETURNING job_id
                    """,
                    image_id,
                    network_id,
                    started_at,
                )
                if not row:
                    raise RuntimeError("Falha ao criar/obter job_id.")
                return int(row["job_id"])
    #todo: make this return image_id
    async def finish_job(
        self,
        *,
        job_id: int,
        finished_at: Optional[datetime] = None,
    ) -> int:
        s = self._schema
        finished_at = finished_at or datetime.now()

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                res = await conn.execute(
                    f"""
                    UPDATE {s}.job
                    SET status = 'finished',
                        finished_at = $2
                    WHERE job_id = $1
                    """,
                    job_id,
                    finished_at,
                )
                if res == "UPDATE 0":
                    raise LookupError(f"job_id={job_id} não existe em {s}.job")
                return image_id

    async def create_image_data(self,*,image_url):
        raise NotImplementedError()

    async def insert_detection_in_final_database(self,*,detection,schema):
        raise NotImplementedError()