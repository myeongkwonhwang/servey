-- saga_schema baseline (Phase 4 ⑥)
-- 소유 서비스: saga-orchestrator (owner)
-- 참고: docs/adr/0001-saga-orchestration.md, A1 흐름 SAGA step 정의

-- saga_state
--   Saga 인스턴스 하나당 1 row. saga-orchestrator 가 INSERT / UPDATE.
--   correlation_key (예: orderProductId) 로 중복 시작 방지 — UNIQUE(saga_type, correlation_key).
--   payload 는 saga context (예: 원본 외부 응답 / step 별 결과) 를 JSONB 로 보존.
--   시각 컬럼은 처음부터 TIMESTAMPTZ (KST 표시 + UTC 저장).
CREATE TABLE saga_schema.saga_state (
    saga_id          UUID         PRIMARY KEY,
    saga_type        VARCHAR(64)  NOT NULL,
    correlation_key  VARCHAR(128) NOT NULL,
    current_step     VARCHAR(64)  NOT NULL,
    status           VARCHAR(32)  NOT NULL,
    payload          JSONB        NOT NULL,
    started_at       TIMESTAMPTZ  NOT NULL,
    updated_at       TIMESTAMPTZ  NOT NULL,
    CONSTRAINT uk_saga_state_correlation
        UNIQUE (saga_type, correlation_key)
);

CREATE INDEX idx_saga_state_status
    ON saga_schema.saga_state (saga_type, status);
