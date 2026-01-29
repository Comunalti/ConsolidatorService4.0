-- init.sql
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS consolidator;

-- =========================
-- DROP (ordem correta)
-- =========================
DROP TABLE IF EXISTS consolidator.detections CASCADE;
DROP TABLE IF EXISTS consolidator.job CASCADE;
DROP TABLE IF EXISTS consolidator.image_data CASCADE;

-- ============================================================
-- IMAGE DATA (TIPADA + JETSON DETECTIONS JSONB)
-- ============================================================
CREATE TABLE consolidator.image_data (
    -- PK interna (sua)
    id BIGSERIAL PRIMARY KEY,

    -- ID externo vindo na mensagem (não é sua PK)
    external_image_id BIGINT NOT NULL,

    -- path externo vindo na mensagem (url)
    image_path TEXT NOT NULL,

    -- tags tipadas
    device    TEXT NOT NULL,
    device_id TEXT NOT NULL,
    camera_id TEXT NOT NULL,
    ibge_code TEXT NOT NULL,

    -- jetson_data tipado
    latitude  DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    jetson_timestamp TIMESTAMP NOT NULL,

    -- detections vindas da Jetson ficam aqui (raw JSON list)
    jetson_detections JSONB NOT NULL DEFAULT '[]'::jsonb,

    -- geom derivada de latitude/longitude
    geom GEOMETRY(Point, 4326) GENERATED ALWAYS AS (
        ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
    ) STORED,

    inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- uniques para dedupe
    CONSTRAINT unique_external_image_id UNIQUE (external_image_id),
    CONSTRAINT unique_image_path UNIQUE (image_path)
);

CREATE INDEX IF NOT EXISTS ix_image_data_geom
  ON consolidator.image_data USING GIST (geom);

CREATE INDEX IF NOT EXISTS ix_image_data_device
  ON consolidator.image_data (device);

CREATE INDEX IF NOT EXISTS ix_image_data_jetson_timestamp
  ON consolidator.image_data (jetson_timestamp);

-- ============================================================
-- JOB (processamento de 1 imagem por 1 "rede"/etapa)
-- ============================================================
CREATE TABLE consolidator.job (
    job_id       BIGSERIAL PRIMARY KEY,

    status       VARCHAR(20) NOT NULL DEFAULT 'processing', -- processing|finished

    image_pk     BIGINT NOT NULL REFERENCES consolidator.image_data(id) ON DELETE CASCADE,

    network_name    TEXT NOT NULL,
    network_version TEXT NOT NULL DEFAULT '1',

    created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at   TIMESTAMP,
    finished_at  TIMESTAMP,
    stored_at    TIMESTAMP,

    CONSTRAINT check_job_status CHECK (status IN ('processing', 'finished'))
);

-- evita job duplicado para mesma imagem/etapa
CREATE UNIQUE INDEX IF NOT EXISTS unique_job_image_network
  ON consolidator.job (image_pk, network_name, network_version);

CREATE INDEX IF NOT EXISTS ix_job_status
  ON consolidator.job (status);

CREATE INDEX IF NOT EXISTS ix_job_network_created_at
  ON consolidator.job (network_name, created_at);

-- ============================================================
-- DETECTIONS (por job) - ALINHADO AO YoloDetection
-- - detection_id é PK do banco
-- - job_id é FK
-- - campos com os mesmos nomes do Pydantic:
--   label, id, confidence, x, y, width, height, image_shape, mask
-- ============================================================
CREATE TABLE consolidator.detections (
    detection_id BIGSERIAL PRIMARY KEY,

    job_id BIGINT NOT NULL REFERENCES consolidator.job(job_id) ON DELETE CASCADE,

    label      TEXT NOT NULL,
    id         INTEGER NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,

    x      DOUBLE PRECISION NOT NULL,
    y      DOUBLE PRECISION NOT NULL,
    width  DOUBLE PRECISION NOT NULL,
    height DOUBLE PRECISION NOT NULL,

    image_shape INTEGER[] NOT NULL,
    mask        JSONB NOT NULL DEFAULT '[]'::jsonb
);

ALTER TABLE consolidator.detections
  ADD CONSTRAINT check_image_shape_len
  CHECK (array_length(image_shape, 1) = 2);

CREATE INDEX IF NOT EXISTS ix_detections_job_id
  ON consolidator.detections (job_id);

CREATE INDEX IF NOT EXISTS ix_detections_id
  ON consolidator.detections (id);






















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




