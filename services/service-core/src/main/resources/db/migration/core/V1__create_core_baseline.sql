-- core_schema baseline (Phase 4 step 3 confirmedOrder)
-- 소유 서비스: service-core (owner)
-- 참고: docs/diagrams/saga-flow.md, project_pcm_step3_confirmedOrder.md

-- 1. orders
--    A1 saga step 3 Pivot 통과 시 INSERT. 보상 미정의 (Pivot 이후 취소는 별도 saga — 다음 라운드 B2).
--    raw 는 외부 응답 원본 보존. shipment_id 는 외부 물류 식별자.
CREATE TABLE core_schema.orders (
    id                          BIGSERIAL    PRIMARY KEY,
    channel                     VARCHAR(32)  NOT NULL,
    external_order_id           VARCHAR(64)  NOT NULL,
    external_order_product_id   VARCHAR(64)  NOT NULL,
    status                      VARCHAR(32)  NOT NULL,
    shipment_id                 VARCHAR(64)  NOT NULL,
    raw                         JSONB        NOT NULL,
    created_at                  TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMP    NOT NULL DEFAULT now(),
    CONSTRAINT uk_orders_channel_external
        UNIQUE (channel, external_order_product_id)
);

CREATE INDEX idx_orders_status
    ON core_schema.orders (channel, status);


-- 2. processed_event (service-core 측 멱등성)
--    동일 ProcessedEvent 도메인 record 를 channel-adapter / service-core 양쪽에서 재사용.
--    JPA Entity 의 @Table 은 schema 명시 X 라 각 service 의 default_schema 가 적용됨.
CREATE TABLE core_schema.processed_event (
    consumer_name   VARCHAR(64)  NOT NULL,
    event_id        VARCHAR(64)  NOT NULL,
    processed_at    TIMESTAMP    NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, event_id)
);
