-- channel_schema 시각 컬럼을 TIMESTAMP (without time zone) 으로 환원
-- 이유:
--   1) 코드 정책 변경 — Instant (UTC) → LocalDateTime (KST 단일 시간대)
--   2) zone 정보를 DB 에 저장하지 않음 — KST 고정 서비스 가정
--   3) [[project-pcm-localdatetime-migration]] 결정 참조
-- 기존 데이터는 TIMESTAMPTZ → KST 로 변환 후 TIMESTAMP 로 환원
--   AT TIME ZONE 'Asia/Seoul' 은 TIMESTAMPTZ → KST wall-clock 으로 변환

ALTER TABLE channel_schema.outbox
    ALTER COLUMN created_at   TYPE TIMESTAMP USING (created_at   AT TIME ZONE 'Asia/Seoul'),
    ALTER COLUMN published_at TYPE TIMESTAMP USING (published_at AT TIME ZONE 'Asia/Seoul');

ALTER TABLE channel_schema.polling_cursor
    ALTER COLUMN window_start TYPE TIMESTAMP USING (window_start AT TIME ZONE 'Asia/Seoul'),
    ALTER COLUMN window_end   TYPE TIMESTAMP USING (window_end   AT TIME ZONE 'Asia/Seoul'),
    ALTER COLUMN updated_at   TYPE TIMESTAMP USING (updated_at   AT TIME ZONE 'Asia/Seoul');

ALTER TABLE channel_schema.staging_order
    ALTER COLUMN ordered_at  TYPE TIMESTAMP USING (ordered_at  AT TIME ZONE 'Asia/Seoul'),
    ALTER COLUMN received_at TYPE TIMESTAMP USING (received_at AT TIME ZONE 'Asia/Seoul');

ALTER TABLE channel_schema.processed_event
    ALTER COLUMN processed_at TYPE TIMESTAMP USING (processed_at AT TIME ZONE 'Asia/Seoul');
