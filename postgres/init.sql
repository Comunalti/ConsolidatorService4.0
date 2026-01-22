-- ============================================================
-- ia_zeladoria (modelo corrigido, mantendo padrão de nomeação antigo)
-- - schema: ia_zeladoria
-- - tabelas: class, network, network_class, image_data, job, detections
-- - classes globais (IDs fixos, ex: 9993, 888880)
-- - network_class guarda local_class_id (índice local do modelo)
-- - detections pertence a job (não direto a image_id)
-- - trigger garante: detections.global_class_id permitido para a network do job
-- ============================================================

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS ia_zeladoria;

-- ============================================================
-- Tabela de Dicionário de Classes (global)
-- Obs: sem SERIAL porque você usa IDs globais fixos
-- ============================================================
CREATE TABLE IF NOT EXISTS ia_zeladoria.class (
    global_class_id INTEGER PRIMARY KEY,
    class_name      VARCHAR(150) NOT NULL,
    description     TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- Tabela de Redes (com version)
-- ============================================================
CREATE TABLE IF NOT EXISTS ia_zeladoria.network (
    network_id      BIGSERIAL PRIMARY KEY,
    network_name    VARCHAR(150) NOT NULL,
    network_version VARCHAR(80)  NOT NULL DEFAULT '1',
    description     TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_network_name_version UNIQUE (network_name, network_version)
);

-- ============================================================
-- Tabela de Classes e Redes
-- - Remove o serial_id (redundante)
-- - Mantém local_class_id
-- - Garante:
--   * (network_id, global_class_id) único (PK)
--   * (network_id, local_class_id) único
-- ============================================================
CREATE TABLE IF NOT EXISTS ia_zeladoria.network_class (
    network_id       BIGINT  NOT NULL REFERENCES ia_zeladoria.network(network_id) ON DELETE CASCADE,
    global_class_id  INTEGER NOT NULL REFERENCES ia_zeladoria.class(global_class_id) ON DELETE RESTRICT,
    local_class_id   INTEGER NOT NULL,

    PRIMARY KEY (network_id, global_class_id),
    CONSTRAINT unique_local_class_network UNIQUE (network_id, local_class_id)
);

CREATE INDEX IF NOT EXISTS ix_network_class_global_class_id
  ON ia_zeladoria.network_class (global_class_id);

-- ============================================================
-- Tabela de Imagem
-- - image_path -> image_url (mas mantém padrão "image_*")
-- - mantém tags, inserted_at, created_at, geom
-- ============================================================
CREATE TABLE IF NOT EXISTS ia_zeladoria.image_data (
    image_id     BIGSERIAL PRIMARY KEY,
    image_url    TEXT NOT NULL UNIQUE,
    tags         JSONB,
    inserted_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    geom         GEOMETRY
);

-- ============================================================
-- Tabela de Jobs (processamento de 1 imagem por 1 rede)
-- ============================================================
CREATE TABLE IF NOT EXISTS ia_zeladoria.job (
    job_id       BIGSERIAL PRIMARY KEY,
    status       VARCHAR(20) NOT NULL DEFAULT 'processing', -- processing|finished
    image_id     BIGINT NOT NULL REFERENCES ia_zeladoria.image_data(image_id) ON DELETE CASCADE,
    network_id   BIGINT NOT NULL REFERENCES ia_zeladoria.network(network_id) ON DELETE RESTRICT,

    created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at   TIMESTAMP,
    finished_at  TIMESTAMP,
    stored_at    TIMESTAMP,

    CONSTRAINT check_job_status CHECK (status IN ('processing', 'finished'))
);

-- evita job duplicado pra mesma (image, network)
CREATE UNIQUE INDEX IF NOT EXISTS unique_job_image_network
  ON ia_zeladoria.job (image_id, network_id);

CREATE INDEX IF NOT EXISTS ix_job_status
  ON ia_zeladoria.job (status);

CREATE INDEX IF NOT EXISTS ix_job_network_created_at
  ON ia_zeladoria.job (network_id, created_at);

-- ============================================================
-- Tabela de Detecções pela Rede (agora por job)
-- - Mantém: confidence, x,y,width,height, mask, created_at, sgc_approved
-- - Remove redundância: image_id não fica aqui (vem via job)
-- ============================================================
CREATE TABLE IF NOT EXISTS ia_zeladoria.detections (
    pass_detections_id BIGSERIAL PRIMARY KEY,

    job_id          BIGINT  NOT NULL REFERENCES ia_zeladoria.job(job_id) ON DELETE CASCADE,
    global_class_id INTEGER NOT NULL REFERENCES ia_zeladoria.class(global_class_id) ON DELETE RESTRICT,

    confidence  NUMERIC(10,6),
    x           NUMERIC(10,6) NOT NULL,
    y           NUMERIC(10,6) NOT NULL,
    width       NUMERIC(10,6) NOT NULL,
    height      NUMERIC(10,6) NOT NULL,

    mask        JSONB,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sgc_approved BOOLEAN NOT NULL DEFAULT FALSE,

    -- similar ao antigo, mas agora por job (não por image_id)
    CONSTRAINT unique_detecion UNIQUE (job_id, global_class_id, x, y, width, height)
);

CREATE INDEX IF NOT EXISTS ix_detections_job_id
  ON ia_zeladoria.detections (job_id);

CREATE INDEX IF NOT EXISTS ix_detections_global_class_id
  ON ia_zeladoria.detections (global_class_id);

-- ============================================================
-- TRIGGER: valida se a classe da detecção é permitida para a network do job
-- ============================================================
CREATE OR REPLACE FUNCTION ia_zeladoria.trg_validate_detection_class_for_job_network()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  v_network_id BIGINT;
BEGIN
  SELECT j.network_id
    INTO v_network_id
    FROM ia_zeladoria.job j
   WHERE j.job_id = NEW.job_id;

  IF v_network_id IS NULL THEN
    RAISE EXCEPTION 'Job % não existe', NEW.job_id
      USING ERRCODE = '23503';
  END IF;

  IF NOT EXISTS (
    SELECT 1
      FROM ia_zeladoria.network_class nc
     WHERE nc.network_id = v_network_id
       AND nc.global_class_id = NEW.global_class_id
  ) THEN
    RAISE EXCEPTION
      'global_class_id % não é permitido para network_id % (job_id %)',
      NEW.global_class_id, v_network_id, NEW.job_id
      USING ERRCODE = '23514';
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS detections_validate_class_for_job_network ON ia_zeladoria.detections;

CREATE TRIGGER detections_validate_class_for_job_network
BEFORE INSERT OR UPDATE OF job_id, global_class_id
ON ia_zeladoria.detections
FOR EACH ROW
EXECUTE FUNCTION ia_zeladoria.trg_validate_detection_class_for_job_network();
