-- channel_schema baseline (Phase 0)
-- 소유 서비스: channel-batch (owner) / channel-adapter (consumer, Flyway disabled)
-- 참고: docs/adr/0001-saga-orchestration.md, A1 흐름 SAGA step 정의

-- 1. outbox
--    Producer: channel-batch (외부 폴링 결과를 이벤트로 적재)
--              + channel-adapter (saga step 결과 이벤트, 후속 라운드)
--    Reader:   Polling Publisher (Message Relay) → Kafka 발행 후 published_at stamp
CREATE TABLE channel_schema.outbox (
    id              BIGSERIAL    PRIMARY KEY,
    aggregate_type  VARCHAR(64)  NOT NULL,
    aggregate_id    VARCHAR(128) NOT NULL,
    event_type      VARCHAR(64)  NOT NULL,
    payload         JSONB        NOT NULL,
    headers         JSONB,
    created_at      TIMESTAMP    NOT NULL DEFAULT now(),
    published_at    TIMESTAMP
);

-- 미발행 이벤트만 빠르게 스캔하기 위한 partial index
CREATE INDEX idx_outbox_unpublished
    ON channel_schema.outbox (id)
    WHERE published_at IS NULL;


-- 2. polling_cursor
--    토스 주문 조회 v2 는 (startDate, endDate) 필수 + cursor 페이지네이션.
--    한 윈도우를 페이지로 나눠 소화하므로 윈도우 + next_cursor 둘 다 보관.
--    채널 × 리소스 단위 single row.
CREATE TABLE channel_schema.polling_cursor (
    channel         VARCHAR(32)  NOT NULL,
    resource        VARCHAR(32)  NOT NULL,
    window_start    TIMESTAMP    NOT NULL,
    window_end      TIMESTAMP    NOT NULL,
    next_cursor     TEXT,
    updated_at      TIMESTAMP    NOT NULL DEFAULT now(),
    PRIMARY KEY (channel, resource)
);


-- 3. staging_order
--    A1 saga step 1 (channel-adapter) 가 INSERT, 보상 시 DELETE.
--    raw JSONB 로 외부 응답 한 건을 통째로 보존 + 자주 조회되는 식별자/상태/시간만 컬럼화.
CREATE TABLE channel_schema.staging_order (
    id                          BIGSERIAL    PRIMARY KEY,
    channel                     VARCHAR(32)  NOT NULL,
    external_order_id           VARCHAR(64)  NOT NULL,
    external_order_product_id   VARCHAR(64)  NOT NULL,
    external_status             VARCHAR(64)  NOT NULL,
    ordered_at                  TIMESTAMP    NOT NULL,
    raw                         JSONB        NOT NULL,
    received_at                 TIMESTAMP    NOT NULL DEFAULT now(),
    CONSTRAINT uk_staging_order_channel_external
        UNIQUE (channel, external_order_product_id)
);

CREATE INDEX idx_staging_order_status
    ON channel_schema.staging_order (channel, external_status);


-- 4. processed_event
--    Consumer 멱등성 (Microservices Patterns Ch.3.3.2).
--    channel-adapter 가 Kafka consume 시 (consumer_name, event_id) 로 dedup.
CREATE TABLE channel_schema.processed_event (
    consumer_name   VARCHAR(64)  NOT NULL,
    event_id        VARCHAR(64)  NOT NULL,
    processed_at    TIMESTAMP    NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, event_id)
);
