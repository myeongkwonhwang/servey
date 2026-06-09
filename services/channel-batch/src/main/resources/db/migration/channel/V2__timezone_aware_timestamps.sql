-- channel_schema 시각 컬럼을 TIMESTAMPTZ 로 전환
-- 이유:
--   1) 코드는 Instant (UTC) 그대로 유지
--   2) PostgreSQL 가 timezone 정보를 함께 보관 → client timezone 으로 변환 표시
--   3) psql / IDE 에서 SET TIME ZONE 'Asia/Seoul' 또는 connection 설정으로 KST 표시
--   4) 글로벌 / 멀티 채널 (UTC 외부 응답) 연동 시 timezone naive 위험 제거
-- 기존 데이터는 UTC naive 로 들어가 있으므로 AT TIME ZONE 'UTC' 변환

ALTER TABLE channel_schema.outbox
    ALTER COLUMN created_at   TYPE TIMESTAMPTZ USING created_at   AT TIME ZONE 'UTC',
    ALTER COLUMN published_at TYPE TIMESTAMPTZ USING published_at AT TIME ZONE 'UTC';

ALTER TABLE channel_schema.polling_cursor
    ALTER COLUMN window_start TYPE TIMESTAMPTZ USING window_start AT TIME ZONE 'UTC',
    ALTER COLUMN window_end   TYPE TIMESTAMPTZ USING window_end   AT TIME ZONE 'UTC',
    ALTER COLUMN updated_at   TYPE TIMESTAMPTZ USING updated_at   AT TIME ZONE 'UTC';

ALTER TABLE channel_schema.staging_order
    ALTER COLUMN ordered_at   TYPE TIMESTAMPTZ USING ordered_at   AT TIME ZONE 'UTC',
    ALTER COLUMN received_at  TYPE TIMESTAMPTZ USING received_at  AT TIME ZONE 'UTC';

ALTER TABLE channel_schema.processed_event
    ALTER COLUMN processed_at TYPE TIMESTAMPTZ USING processed_at AT TIME ZONE 'UTC';
