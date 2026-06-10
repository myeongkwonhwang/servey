-- saga_schema 시각 컬럼을 TIMESTAMP (without time zone) 으로 환원
-- 이유: 코드 정책 변경 — Instant → LocalDateTime (KST 단일 시간대)
-- 기존 데이터는 TIMESTAMPTZ → KST 로 변환 후 TIMESTAMP 로 환원

ALTER TABLE saga_schema.saga_state
    ALTER COLUMN started_at TYPE TIMESTAMP USING (started_at AT TIME ZONE 'Asia/Seoul'),
    ALTER COLUMN updated_at TYPE TIMESTAMP USING (updated_at AT TIME ZONE 'Asia/Seoul');
