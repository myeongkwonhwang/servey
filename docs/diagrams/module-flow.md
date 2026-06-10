# 모듈 단위 흐름도

본 프로젝트의 4 Spring Boot app + 6 libs 구조를 모듈 단위로 시각화. 각 모듈의 책임 / 통신 / 의존을 한 페이지에서 파악.

관련 결정 메모리:
- `project_pcm_4app_architecture_decision.md` (구조)
- `project_pcm_flow_only_topics.md` (Kafka 토픽 구조)
- `project_pcm_logistics_gateway_naming.md` (Gateway 결정)
- `project_pcm_saga_pivot_compensation_summary.md` (Pivot / 보상)

---

## 1. App 구성 (4 Spring Boot) + 책임

```mermaid
flowchart LR
    subgraph EXT["외부 시스템"]
        TOSS["TOSS 판매 채널<br/>(HTTP REST API)"]
        LOGI["자사 물류 시스템<br/>(DB-to-DB Interface)"]
    end

    subgraph APP["4 Spring Boot Apps"]
        CB["channel-batch<br/>:8081<br/>외부 폴링 + outbox publisher"]
        CA["channel-adapter<br/>:8082<br/>외부 WRITE + consumer"]
        SC["service-core<br/>:8083<br/>도메인 (주문/클레임)"]
        SO["saga-orchestrator<br/>:8085<br/>saga state + 흐름 조율"]
    end

    subgraph INFRA["인프라"]
        K["Kafka<br/>channel.order.received<br/>saga.order.cmd/reply"]
        DB[("PostgreSQL<br/>channel_schema<br/>core_schema<br/>saga_schema")]
    end

    TOSS -->|"GET /orders/v2"| CB
    CB --> K
    K --> CA
    K --> SC
    K --> SO
    CA -->|"PUT /products/status<br/>POST /seller-cancel"| TOSS
    SC -->|"Gateway"| LOGI

    CB -.-> DB
    CA -.-> DB
    SC -.-> DB
    SO -.-> DB

    classDef ext fill:#fff3e0,stroke:#e65100
    classDef app fill:#e3f2fd,stroke:#0d47a1
    classDef infra fill:#f3e5f5,stroke:#4a148c
    class TOSS,LOGI ext
    class CB,CA,SC,SO app
    class K,DB infra
```

### App 별 책임 매트릭스

| App | 외부 통신 | DB schema (owner) | Kafka 역할 | saga 책임 |
|---|---|---|---|---|
| **channel-batch** | TOSS GET (READ) | channel_schema (owner) | producer (`channel.order.received`) | A1 트리거 (saga 외부) |
| **channel-adapter** | TOSS PUT/POST (WRITE) | channel_schema (read-only) | consumer (`saga.order.cmd` 일부) + publisher (`saga.order.reply`) | step 1 정상 + step 1 보상 |
| **service-core** | 자사 물류 Gateway | core_schema (owner) | consumer (`saga.order.cmd` 일부) + publisher (`saga.order.reply`) | step 2 validate + step 3 Pivot |
| **saga-orchestrator** | — | saga_schema (owner) | consumer (`channel.order.received`, `saga.order.reply`) + publisher (`saga.order.cmd`) | saga state + 모든 step 조율 + 보상 트리거 |

---

## 2. libs 의존 관계 (6 libs)

```mermaid
flowchart BT
    CD["common-domain<br/>(가장 안쪽)"]
    CES["channel-event-schema<br/>(saga DTO / 상수)"]
    CC["channel-client<br/>(TOSS HTTP)"]
    KI["kafka-infra<br/>(Kafka 공통)"]
    LG["logistics-gateway<br/>(자사 물류 DB-to-DB)"]
    PC["persistence-common<br/>(JPA / Outbox / SagaState)"]

    CES --> CD
    CC --> CD
    CC --> CES
    KI --> CD
    KI --> CES
    LG --> CD
    PC --> CD

    classDef inner fill:#fff9c4,stroke:#f57f17
    classDef wrap fill:#e8f5e9,stroke:#1b5e20
    class CD inner
    class CES,CC,KI,LG,PC wrap
```

### App → libs 의존

| App | common-domain | channel-event-schema | channel-client | kafka-infra | logistics-gateway | persistence-common |
|---|---|---|---|---|---|---|
| channel-batch | ✓ | ✓ | ✓ | ✓ | | ✓ |
| channel-adapter | ✓ | ✓ | ✓ | ✓ | | ✓ |
| service-core | ✓ | ✓ | | ✓ | ✓ | ✓ |
| saga-orchestrator | ✓ | ✓ | | ✓ | | ✓ |

### 의존 규칙 (ArchUnit 강제 예정)
- `services/*` → `libs/*` 만
- `libs/*` → 다른 `libs/*` 만 (services 의존 금지)
- `services/*` 간 직접 의존 금지 — Kafka 통신만

---

## 3. A1 saga — 모듈 간 sequence (정상 흐름)

```mermaid
sequenceDiagram
    autonumber
    participant Toss as 외부 TOSS API
    participant CB as channel-batch
    participant K1 as Kafka<br/>channel.order.received
    participant SO as saga-orchestrator
    participant K2 as Kafka<br/>saga.order.cmd
    participant K3 as Kafka<br/>saga.order.reply
    participant CA as channel-adapter
    participant SC as service-core
    participant LG as 자사 물류 Gateway

    Note over Toss,CB: step 0 — orderReception (트리거)
    Toss->>CB: GET /orders/v2 (PAID 폴링)
    CB->>K1: outbox INSERT → publish

    Note over K1,SO: A1 saga 시작
    K1->>SO: ChannelOrderReceivedConsumer
    SO->>SO: SagaStarter.startIfAbsent<br/>(saga_state INSERT, STARTED)
    SO->>K2: UnconfirmedOrderCommand<br/>(UNCONFIRMED_ORDER_REQUEST)
    SO->>SO: advance UNCONFIRMED_ORDER_SENT

    Note over K2,CA: step 1 — unconfirmedOrder
    K2->>CA: UnconfirmedOrderCommandConsumer<br/>(정확 매칭)
    CA->>CA: BasicValidator (fast fail)
    CA->>Toss: PUT /orders/products/status<br/>(PAID → PREPARING_PRODUCT)
    CA->>CA: staging INSERT + processed_event (Tx)
    CA->>K3: UnconfirmedOrderReply OK
    K3->>SO: UnconfirmedOrderReplyConsumer
    SO->>SO: advance UNCONFIRMED_ORDER_INSERTED
    SO->>K2: ValidateCommand
    SO->>SO: advance VALIDATE_SENT

    Note over K2,SC: step 2 — validate (read-only)
    K2->>SC: ValidateCommandConsumer
    SC->>SC: Validator (orderProductStatus=PAID?)
    SC->>K3: ValidateReply OK
    K3->>SO: ValidateReplyConsumer
    SO->>SO: advance VALIDATED
    SO->>K2: ConfirmedOrderCommand
    SO->>SO: advance CONFIRMED_ORDER_SENT

    Note over K2,SC: step 3 — confirmedOrder (Pivot ★)
    K2->>SC: ConfirmedOrderCommandConsumer
    SC->>LG: LogisticsGateway.request<br/>(Tx 밖)
    LG-->>SC: shipmentId
    SC->>SC: orders INSERT + processed_event (Tx)<br/>= Pivot 통과
    SC->>K3: ConfirmedOrderReply OK
    K3->>SO: ConfirmedOrderReplyConsumer
    SO->>SO: advance CONFIRMED_ORDER_PERSISTED
```

---

## 4. A1 saga 보상 흐름 (Round B — step 2 FAIL 시)

```mermaid
sequenceDiagram
    autonumber
    participant SO as saga-orchestrator
    participant K2 as Kafka<br/>saga.order.cmd
    participant K3 as Kafka<br/>saga.order.reply
    participant CA as channel-adapter
    participant Toss as 외부 TOSS API

    Note over SO: step 2 FAIL — 보상 시작
    Note over SO: 사전 상태: VALIDATE_SENT
    SO->>SO: advance VALIDATE_FAILED
    SO->>SO: compensate (status=COMPENSATING) ★
    SO->>SO: sagaState.findById → payload<br/>→ orderProductId 추출
    SO->>K2: UnconfirmedOrderCompensateCommand<br/>(UNCONFIRMED_ORDER_COMPENSATE_REQUEST)
    SO->>SO: advance UNCONFIRMED_ORDER_COMPENSATE_SENT

    K2->>CA: UnconfirmedOrderCompensateCommandConsumer<br/>(정확 매칭, 정상 step consumer 와 분리)

    Note over CA,Toss: 사용자 결정 순서 — 외부 먼저
    CA->>Toss: POST /seller-cancel<br/>(PREPARING_PRODUCT → CANCELED_PAYMENT)
    Toss-->>CA: SUCCESS

    Note over CA: 외부 OK 확인 후 내부 동기화
    CA->>CA: StagingOrderCancelPersister.cancelIfPresent (Tx)<br/>staging.markCanceled (status=CANCELED) +<br/>processed_event INSERT

    CA->>K3: UnconfirmedOrderCompensateReply OK<br/>(_COMPENSATE_REPLY_OK)
    K3->>SO: UnconfirmedOrderCompensateReplyConsumer<br/>(prefix UNCONFIRMED_ORDER_COMPENSATE_REPLY)
    SO->>SO: advance UNCONFIRMED_ORDER_COMPENSATED
    SO->>SO: abort (status=ABORTED) ★

    Note over SO: saga 종결 — ABORTED
```

**보상 순서 (사용자 결정)** — 외부가 진실 소스
1. 외부 seller-cancel API
2. 성공 시 내부 staging UPDATE 동기화
3. 외부 실패 시 reply FAIL → saga 가 COMPENSATING 유지 (재시도 / 수동 개입)

---

## 5. DB schema 책임

| Schema | Owner App | 사용 App | 주요 테이블 |
|---|---|---|---|
| `channel_schema` | channel-batch | channel-batch (write), channel-adapter (write) | outbox, polling_cursor, staging_order, processed_event |
| `core_schema` | service-core | service-core (write) | orders, processed_event |
| `saga_schema` | saga-orchestrator | saga-orchestrator (write) | saga_state |

**정책**:
- Owner 가 Flyway migration 수행. 다른 service 는 `flyway.enabled: false` + JPA `validate`
- `processed_event` 는 schema 명시 없는 JPA Entity 로 공유. 각 service 의 `hibernate.default_schema` 따라 channel_schema / core_schema 에 별도 테이블

---

## 6. 외부 통신 매트릭스

| 외부 시스템 | App | 통신 방식 | 사용처 (saga step) | Mock 위치 |
|---|---|---|---|---|
| TOSS GET /orders/v2 | channel-batch | HTTP READ | step 0 (트리거) | `MockTossOrderClient` (channel-batch) |
| TOSS PUT /products/status | channel-adapter | HTTP WRITE | step 1 (PAID→PREPARING) | `MockTossOrderStatusClient` (channel-adapter) |
| TOSS POST /seller-cancel | channel-adapter | HTTP WRITE | step 1 보상 (→ CANCELED_PAYMENT) | `MockTossOrderCancelClient` (channel-adapter) |
| 자사 물류 | service-core | DB-to-DB (Gateway) | step 3 (Pivot) | `MockLogisticsGateway` (service-core) |

---

## 7. Round 진행 요약 (구현 현황)

| Round | 범위 | 상태 |
|---|---|---|
| Phase 0 | 4 app + 5 libs 골격 | ✅ |
| Phase 4 ① ~ ⑥ | Flyway / Outbox / Publisher / Consumer / saga state | ✅ |
| Phase 4 ⑦ | step 1 saga command/reply 흐름 | ✅ |
| Saga 재정의 | A1 4 step + B1 분리 | ✅ |
| flow only 토픽 | saga.order.cmd/reply 두 토픽 통합 | ✅ |
| step 2 validate | service-core 합류 | ✅ |
| step 3 confirmedOrder | service-core Pivot 통과 | ✅ |
| step 1 확장 | BasicValidator + 외부 PREPARING_PRODUCT | ✅ |
| LocalDateTime migration | Instant + TIMESTAMPTZ → LocalDateTime + TIMESTAMP | ✅ |
| LogisticsGateway 분리 | 6 libs 로 확장 | ✅ |
| Round B 보상 | step 2 FAIL → 외부 cancel + staging UPDATE → ABORTED | ✅ |
| Pivot 직전 자동 cancel | step 3 부분 실패 시 외부 cancel | 🟡 계획 |
| B1 sendInvoice | 물류 회신 saga | 🟡 계획 |
| B2 취소 saga | Pivot 이후 사용자 취소 | 🟡 계획 |
| outbox 통합 | saga reply 발행 + 보상 outbox 화 | 🟡 계획 |
| 다채널 지원 | saga state 채널 컬럼 / Strategy | 🟡 계획 |
