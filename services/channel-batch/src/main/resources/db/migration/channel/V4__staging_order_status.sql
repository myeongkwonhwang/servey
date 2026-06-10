-- channel_schema.staging_order 에 status 컬럼 추가
-- A1 saga step 2 FAIL 시 보상 흐름에서 row UPDATE 로 취소 표시 (DELETE 가 아닌 감사 추적 유지)
-- 기존 row 는 ACTIVE 로 backfill

ALTER TABLE channel_schema.staging_order
    ADD COLUMN status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE';

CREATE INDEX idx_staging_order_active_status
    ON channel_schema.staging_order (channel, status);
