-- ============================================================
-- consolidator (antigo ia_zeladoria)
-- - schema: consolidator
-- - tabelas: class, network, network_class, image_data, job, detections
-- - classes globais (IDs fixos, ex: 9993, 888880)
-- - network_class guarda local_class_id (índice local do modelo)
-- - detections pertence a job (não direto a image_id)
-- - trigger garante: detections.global_class_id permitido para a network do job
-- ============================================================

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS consolidator;

-- ============================================================
-- Tabela de Dicionário de Classes (global)
-- Obs: sem SERIAL porque você usa IDs globais fixos
-- ============================================================
CREATE TABLE IF NOT EXISTS consolidator.class (
    global_class_id INTEGER PRIMARY KEY,
    class_name      VARCHAR(150) NOT NULL,
    description     TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- Tabela de Redes (com version)
-- ============================================================
CREATE TABLE IF NOT EXISTS consolidator.network (
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
CREATE TABLE IF NOT EXISTS consolidator.network_class (
    network_id       BIGINT  NOT NULL REFERENCES consolidator.network(network_id) ON DELETE CASCADE,
    global_class_id  INTEGER NOT NULL REFERENCES consolidator.class(global_class_id) ON DELETE RESTRICT,
    local_class_id   INTEGER NOT NULL,

    PRIMARY KEY (network_id, global_class_id),
    CONSTRAINT unique_local_class_network UNIQUE (network_id, local_class_id)
);

CREATE INDEX IF NOT EXISTS ix_network_class_global_class_id
  ON consolidator.network_class (global_class_id);

-- ============================================================
-- Tabela de Imagem
-- - image_path (renomeado)
-- - mantém tags, inserted_at, created_at, geom
-- ============================================================
CREATE TABLE IF NOT EXISTS consolidator.image_data (
    image_id     BIGSERIAL PRIMARY KEY,
    image_path   TEXT NOT NULL UNIQUE,
    tags         JSONB NOT NULL DEFAULT '{}'::jsonb,
    inserted_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    geom         GEOMETRY NOT NULL DEFAULT ST_SetSRID(ST_MakePoint(0,0), 4326)
);

-- ============================================================
-- Tabela de Jobs (processamento de 1 imagem por 1 rede)
-- ============================================================
CREATE TABLE IF NOT EXISTS consolidator.job (
    job_id       BIGSERIAL PRIMARY KEY,
    status       VARCHAR(20) NOT NULL DEFAULT 'processing', -- processing|finished
    image_id     BIGINT NOT NULL REFERENCES consolidator.image_data(image_id) ON DELETE CASCADE,
    network_id   BIGINT NOT NULL REFERENCES consolidator.network(network_id) ON DELETE RESTRICT,

    created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at   TIMESTAMP,
    finished_at  TIMESTAMP,
    stored_at    TIMESTAMP,

    CONSTRAINT check_job_status CHECK (status IN ('processing', 'finished'))
);

-- evita job duplicado pra mesma (image, network)
CREATE UNIQUE INDEX IF NOT EXISTS unique_job_image_network
  ON consolidator.job (image_id, network_id);

CREATE INDEX IF NOT EXISTS ix_job_status
  ON consolidator.job (status);

CREATE INDEX IF NOT EXISTS ix_job_network_created_at
  ON consolidator.job (network_id, created_at);

-- ============================================================
-- Tabela de Detecções pela Rede (agora por job)
-- - Mantém: confidence, x,y,width,height, mask, created_at, sgc_approved
-- - Remove redundância: image_id não fica aqui (vem via job)
-- ============================================================
CREATE TABLE IF NOT EXISTS consolidator.detections (
    pass_detections_id BIGSERIAL PRIMARY KEY,

    job_id          BIGINT  NOT NULL REFERENCES consolidator.job(job_id) ON DELETE CASCADE,
    global_class_id INTEGER NOT NULL REFERENCES consolidator.class(global_class_id) ON DELETE RESTRICT,

    label       TEXT NOT NULL DEFAULT '',

    confidence  NUMERIC(10,6),
    x           NUMERIC(10,6) NOT NULL,
    y           NUMERIC(10,6) NOT NULL,
    width       NUMERIC(10,6) NOT NULL,
    height      NUMERIC(10,6) NOT NULL,

    mask        JSONB,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sgc_approved BOOLEAN NOT NULL DEFAULT FALSE,

    CONSTRAINT unique_detecion UNIQUE (job_id, global_class_id, x, y, width, height)
);


CREATE INDEX IF NOT EXISTS ix_detections_job_id
  ON consolidator.detections (job_id);

CREATE INDEX IF NOT EXISTS ix_detections_global_class_id
  ON consolidator.detections (global_class_id);

-- ============================================================
-- TRIGGER: valida se a classe da detecção é permitida para a network do job
-- ============================================================
CREATE OR REPLACE FUNCTION consolidator.trg_validate_detection_class_for_job_network()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  v_network_id BIGINT;
BEGIN
  -- Busca a network do job (usando o schema consolidator)
  SELECT j.network_id
    INTO v_network_id
    FROM consolidator.job j
   WHERE j.job_id = NEW.job_id;

  IF v_network_id IS NULL THEN
    RAISE EXCEPTION 'Job % não existe', NEW.job_id
      USING ERRCODE = '23503';
  END IF;

  -- Verifica se a classe global é mapeada para essa network
  IF NOT EXISTS (
    SELECT 1
      FROM consolidator.network_class nc
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

DROP TRIGGER IF EXISTS detections_validate_class_for_job_network ON consolidator.detections;

CREATE TRIGGER detections_validate_class_for_job_network
BEFORE INSERT OR UPDATE OF job_id, global_class_id
ON consolidator.detections
FOR EACH ROW
EXECUTE FUNCTION consolidator.trg_validate_detection_class_for_job_network();



























CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS ia_zeladoria;


-- Tabela de Dicionário de Classes
CREATE TABLE ia_zeladoria.class (
    global_class_id SERIAL PRIMARY KEY,  -- Chave primária, autoincrementada
    class_name VARCHAR(100) NOT NULL,
    description TEXT
);

-- Tabela de Redes
CREATE TABLE ia_zeladoria.network (
    network_id SERIAL PRIMARY KEY,  -- Chave primária, autoincrementada
    network_name VARCHAR(100) NOT NULL,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Classes e Redes
CREATE TABLE ia_zeladoria.network_class (
    serial_id SERIAL PRIMARY KEY,  -- Chave primária, autoincrementada
    global_class_id INTEGER NOT NULL REFERENCES ia_zeladoria.class(global_class_id),  -- Relacionamento com ia_zeladoria.class
    local_class_id INTEGER NOT NULL,
    network_id INTEGER NOT NULL REFERENCES ia_zeladoria.network(network_id),  -- Relacionamento com ia_zeladoria.network
    CONSTRAINT unique_class_network UNIQUE (local_class_id, network_id)  -- Garantia de combinação única
);

-- Tabela de Imagem
CREATE TABLE ia_zeladoria.image_data (
    image_id SERIAL PRIMARY KEY,  -- Chave primária, autoincrementada
    image_path TEXT NOT NULL,
    tags JSONB,
    inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    geom GEOMETRY
);

-- Tabela de Detecções pela Rede
CREATE TABLE ia_zeladoria.detections (
    pass_detections_id SERIAL PRIMARY KEY,  -- Chave primária, autoincrementada
    image_id INTEGER NOT NULL REFERENCES ia_zeladoria.image_data(image_id),  -- Relacionamento com ia_zeladoria.image_data
    global_class_id INTEGER NOT NULL REFERENCES ia_zeladoria.class(global_class_id),  -- Relacionamento com ia_zeladoria.class
    confidence NUMERIC(10,6),
    x NUMERIC(10,6),
    y NUMERIC(10,6),
    width NUMERIC(10,6),
    height NUMERIC(10,6),
    mask JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sgc_approved BOOLEAN DEFAULT false,
    CONSTRAINT unique_detecion UNIQUE (image_id, global_class_id,x,y,width,height)  -- Garantia de combinação única
);





-- 1. Inserir as Redes
INSERT INTO ia_zeladoria.network (network_name, description)
VALUES
  ('RedeMae', 'RedeMae network'),
  ('ArvoreElementosDePoste', 'ArvoreElementosDePoste network'),
  ('ElementosDePoste', 'ElementosDePoste network'),
  ('FaixaPedestreTipos', 'FaixaPedestreTipos network'),
  ('FissurasTipos', 'FissurasTipos network'),
  ('PlacasDeSinalizacaoTipos', 'PlacasDeSinalizacaoTipos network'),
  ('QualidadeBocaDeLobo', 'QualidadeBocaDeLobo network'),
  ('QualidadeSarjetao', 'QualidadeSarjetao network'),
  ('BuracoProfundidadeCuritiba', 'BuracoProfundidadeCuritiba network'),
  ('PoluicaoNaVia', 'PoluicaoNaVia network'),
  ('Background', 'Background network'),
  ('QualidadeTampaPV', 'QualidadeTampaPV network');

-- 2. Inserir as Classes (cada linha é única pelo global_class_id conforme o JSON)
INSERT INTO ia_zeladoria.class (global_class_id, class_name, description)
VALUES
  -- RedeMae
  (0, 'Lombada', 'Lombada'),
  (1, 'Sarjetao', 'Sarjetao'),
  (2, 'Buraco', 'Buraco'),
  (9993, 'Tampa de PV Adequada RM', 'Tampa de PV Adequada RM'),
  (9994, 'Tampa de PV com Defeito RM', 'Tampa de PV com Defeito RM'),
  (5, 'Recomposicao Asfaltica', 'Recomposicao Asfaltica'),
  (9996, 'Fissura Couro de Jacare RM', 'Fissura Couro de Jacare RM'),
  (9997, 'Fissura Transversal RM', 'Fissura Transversal RM'),
  (9998, 'Fissura Longitudinal RM', 'Fissura Longitudinal RM'),
  (9, 'Boca de Lobo', 'Boca de Lobo'),
  (10, 'Sarjeta/ Drenagem com Defeito', 'Sarjeta/ Drenagem com Defeito'),
  (99911, 'Placa de Regulamentacao RM', 'Placa de Regulamentacao RM'),
  (99912, 'Placa de Advertencia RM', 'Placa de Advertencia RM'),
  (99913, 'Placa De Indicao RM', 'Placa De Indicao RM'),
  (99914, 'Placa Educativa RM', 'Placa Educativa RM'),
  (99915, 'Placa Auxiliar RM', 'Placa Auxiliar RM'),
  (16, 'Lixeira Inadequada RM', 'Lixeira Inadequada RM'),
  (17, 'Hidrante', 'Hidrante'),
  (18, 'Paralelepipedo', 'Paralelepipedo'),
  (19, 'Boca de Leao', 'Boca de Leao'),
  (99920, 'Sombra', 'Sombra'),
  (99921, 'Tachao', 'Tachao'),
  (99922, 'Fantasma', 'Fantasma'),
  (23, 'Sinalizacao Horizontal', 'Sinalizacao Horizontal'),
  (24, 'Placa', 'Placa'),
  (25, 'Fissura', 'Fissura'),
  (26, 'Tampa de PV', 'Tampa de PV'),

  -- ArvoreElementosDePoste
  (51, 'Excesso de Fios', 'Excesso de Fios'),
  (50, 'arvore', 'arvore'),
  (54, 'Luminaria', 'Luminaria'),
  (55, 'Luminaria Acesa de Dia', 'Luminaria Acesa de Dia'),

  -- ElementosDePoste
  (35, 'Poste', 'Poste'),
  (36, 'Lixeira Adequada', 'Lixeira Adequada'),
  (37, 'Lixeira Inadequada', 'Lixeira Inadequada'),
  (38, 'Lixeira Transbordando', 'Lixeira Transbordando'),
  (39, 'Galhardete', 'Galhardete'),

  -- FaixaPedestreTipos
  (52, 'Faixa de Pedestre', 'Faixa de Pedestre'),
  (53, 'Faixa de Pedestre Inadequada', 'Faixa de Pedestre Inadequada'),

  -- FissurasTipos
  (6, 'Fissura Couro de Jacare', 'Fissura Couro de Jacare'),
  (7, 'Fissura Transversal', 'Fissura Transversal'),
  (8, 'Fissura Longitudinal', 'Fissura Longitudinal'),

  -- PlacasDeSinalizacaoTipos
  (11, 'Placa de Regulamentacao', 'Placa de Regulamentacao'),
  (12, 'Placa de Advertencia', 'Placa de Advertencia'),
  (13, 'Placa De Indicao', 'Placa De Indicao'),
  (14, 'Placa Educativa', 'Placa Educativa'),
  (15, 'Placa Auxiliar', 'Placa Auxiliar'),

  -- QualidadeBocaDeLobo
  (27, 'Boca de Lobo Adequada', 'Boca de Lobo Adequada'),
  (28, 'Boca de Lobo Inadequada', 'Boca de Lobo Inadequada'),

  -- QualidadeSarjetao
  (29, 'Sarjetao Adequado', 'Sarjetao Adequado'),
  (30, 'Sarjetao Inadequado', 'Sarjetao Inadequado'),
  (41, 'Sarjeta', 'Sarjeta'),

  -- BuracoProfundidadeCuritiba
  (31, 'Buraco Profundo', 'Buraco Profundo'),
  (32, 'Buraco Superficial', 'Buraco Superficial'),

  -- PoluicaoNaVia
  (33, 'Entulho', 'Entulho'),
  (40, 'Saco de Varricao', 'Saco de Varricao'),
  (49, 'Grama na Sarjeta', 'Grama na Sarjeta'),

  -- Background
  (888880, 'rua', 'rua'),
  (888881, 'calcada', 'calcada'),
  (888882, 'canteiro', 'canteiro'),
  (888883, 'partes do carro', 'partes do carro'),
  (888884, 'reflexo', 'reflexo'),
  (888885, 'fundo', 'fundo'),

  -- QualidadeTampaPV
  (3, 'Tampa de PV Adequada', 'Tampa de PV Adequada'),
  (4, 'Tampa de PV com Defeito', 'Tampa de PV com Defeito');

-- 3. Inserir a associação entre Classes e Redes conforme os índices locais (local_class_id)
-- RedeMae
INSERT INTO ia_zeladoria.network_class (global_class_id, local_class_id, network_id)
VALUES
  (0,  0, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (1,  1, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (2,  2, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (9993,  3, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (9994,  4, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (5,  5, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (9996,  6, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (9997,  7, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (9998,  8, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (9,  9, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (10, 10, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (99911, 11, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (99912, 12, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (99913, 13, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (99914, 14, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (99915, 15, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (16, 16, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (17, 17, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (18, 18, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (19, 19, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (99920, 20, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (99921, 21, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (99922, 22, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (23, 23, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (24, 24, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (25, 25, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae')),
  (26, 26, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'RedeMae'));

-- ArvoreElementosDePoste
INSERT INTO ia_zeladoria.network_class (global_class_id, local_class_id, network_id)
VALUES
  (51, 0, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'ArvoreElementosDePoste')),
  (50, 1, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'ArvoreElementosDePoste')),
  (54, 2, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'ArvoreElementosDePoste')),
  (55, 3, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'ArvoreElementosDePoste'));

-- ElementosDePoste
INSERT INTO ia_zeladoria.network_class (global_class_id, local_class_id, network_id)
VALUES
  (35, 0, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'ElementosDePoste')),
  (36, 1, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'ElementosDePoste')),
  (37, 2, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'ElementosDePoste')),
  (38, 3, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'ElementosDePoste')),
  (39, 4, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'ElementosDePoste'));

-- FaixaPedestreTipos
INSERT INTO ia_zeladoria.network_class (global_class_id, local_class_id, network_id)
VALUES
  (52, 0, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'FaixaPedestreTipos')),
  (53, 1, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'FaixaPedestreTipos'));

-- FissurasTipos
INSERT INTO ia_zeladoria.network_class (global_class_id, local_class_id, network_id)
VALUES
  (6,  0, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'FissurasTipos')),
  (7,  1, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'FissurasTipos')),
  (8,  2, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'FissurasTipos'));

-- PlacasDeSinalizacaoTipos
INSERT INTO ia_zeladoria.network_class (global_class_id, local_class_id, network_id)
VALUES
  (11, 0, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'PlacasDeSinalizacaoTipos')),
  (12, 1, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'PlacasDeSinalizacaoTipos')),
  (13, 2, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'PlacasDeSinalizacaoTipos')),
  (14, 3, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'PlacasDeSinalizacaoTipos')),
  (15, 4, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'PlacasDeSinalizacaoTipos'));

-- QualidadeBocaDeLobo
INSERT INTO ia_zeladoria.network_class (global_class_id, local_class_id, network_id)
VALUES
  (27, 0, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'QualidadeBocaDeLobo')),
  (28, 1, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'QualidadeBocaDeLobo'));

-- QualidadeSarjetao
INSERT INTO ia_zeladoria.network_class (global_class_id, local_class_id, network_id)
VALUES
  (29, 0, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'QualidadeSarjetao')),
  (30, 1, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'QualidadeSarjetao')),
  (41, 2, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'QualidadeSarjetao'));

-- BuracoProfundidadeCuritiba
INSERT INTO ia_zeladoria.network_class (global_class_id, local_class_id, network_id)
VALUES
  (31, 0, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'BuracoProfundidadeCuritiba')),
  (32, 1, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'BuracoProfundidadeCuritiba'));

-- PoluicaoNaVia
INSERT INTO ia_zeladoria.network_class (global_class_id, local_class_id, network_id)
VALUES
  (33, 0, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'PoluicaoNaVia')),
  (40, 1, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'PoluicaoNaVia')),
  (49, 2, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'PoluicaoNaVia'));

-- Background
INSERT INTO ia_zeladoria.network_class (global_class_id, local_class_id, network_id)
VALUES
  (888880, 0, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'Background')),
  (888881, 1, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'Background')),
  (888882, 2, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'Background')),
  (888883, 3, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'Background')),
  (888884, 4, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'Background')),
  (888885, 5, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'Background'));

-- QualidadeTampaPV
INSERT INTO ia_zeladoria.network_class (global_class_id, local_class_id, network_id)
VALUES
  (3, 0, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'QualidadeTampaPV')),
  (4, 1, (SELECT network_id FROM ia_zeladoria.network WHERE network_name = 'QualidadeTampaPV'));




