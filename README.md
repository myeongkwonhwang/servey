# partner-channel-msa

멀티채널 (쿠팡 / 네이버 / 유튜브쇼핑 / 토스쇼핑 등) 제휴 주문/클레임 흐름을 **MSA + 분산 SAGA** 기반으로 설계하는 학습 프로젝트.

## 정체성

| 항목 | 내용 |
|------|------|
| **목적** | 멀티채널 주문/클레임 통합을 MSA + 분산 SAGA 패턴으로 학습 |
| **범위 (In-Scope)** | 외부 채널 수신 · 판매 가능 검증 · 내부 주문 생성 · 물류 전송 · 송장 연동 · 취소/반품/교환 |
| **비범위 (Out-of-Scope)** | 정산 / 알림 / 통계 (후행 흐름 — 추후 코레오로 확장 가능) |
| **아키텍처 한 줄** | 4 services + 5 libs Multi-gradle. Pragmatic MSA (도메인 경계 모호 영역은 한 service 안 모듈 보존) |

## 왜 만드는가

흔한 멀티채널 통합 시스템의 패턴들을 책 / 사례 기반으로 학습:
- **Anti-Corruption Layer** — 외부 채널을 도메인 모델로 정규화
- **Transactional Outbox + Idempotent Consumer** — 이중 쓰기 방지 + 멱등성
- **분산 SAGA** — 외부/내부 트랜잭션 조율 + 보상
- **Multi-tenant Channel Strategy** — 채널 N 개 확장성
- **Database per Service (논리)** — 옵션 A: 같은 DB + schema 분리

## 구조

```
partner-channel-msa/
├── settings.gradle              # 9 modules
├── build.gradle                 # Java 21 + Spring Boot 3.4.5 + Lombok + ArchUnit
├── .gitignore
│
├── services/                    # Spring Boot × 4
│   ├── channel-batch/           # publisher — 외부 + 내부 polling
│   │   └── batch/external · internal/{ordr,claim}
│   ├── channel-adapter/         # consumer + 외부 WRITE (multi-tenant Strategy)
│   ├── service-core/            # consumer — ordr + claim 도메인 처리
│   │   └── core/{ordr,claim}    # 모듈 경계 (ArchUnit 강제 예정)
│   └── saga-orchestrator/       # 분산 SAGA 상태머신 + 보상
│
└── libs/                        # java-library × 5
    ├── common-domain/           # 가장 안쪽 — 공통 ID / 도메인 모델
    ├── channel-event-schema/    # Kafka 이벤트 DTO
    ├── channel-client/          # Feign / Token / RateLimit / Recovery / Strategy
    ├── kafka-infra/             # Outbox / Idempotent Consumer 유틸
    └── persistence-common/      # processed_event 등
```

## 진행 단계

- [v] Phase 0 골격 (28 files)
- [v] 서비스 구성 결정 — 4 app + 5 libs
- [v] SAGA Q1 — 오케 일관 (후행 흐름 추가 시 하이브리드 전환)
- [v] A1 흐름 (외부 주문 수신) SAGA step 정의 — 4 step + Pivot at step 4
- [ ] SAGA Q2 (구현 방식 — 자체 / Spring Statemachine / Eventuate / Temporal / Camunda)
- [ ] SAGA Q4 (보상 멱등성 — Idempotency-Key / Outbox dedup)
- [ ] Gradle wrapper + 첫 빌드 검증 + git init
- [ ] 의존성 추가 (Kafka / JPA·MyBatis / Feign / Flyway 등)
- [ ] 첫 코드 — A1 흐름 ① channel-batch Scheduler 부터

## 문서

| 문서 | 내용 |
|------|------|
| [docs/adr/0001-saga-style.md](docs/adr/0001-saga-style.md) | SAGA 스타일 (Q1 결정 반영 예정) |
| [docs/adr/0002-service-split.md](docs/adr/0002-service-split.md) | 서비스 분할 (4 app 으로 갱신 예정) |
| [docs/as-is/toss-shopping.md](docs/as-is/toss-shopping.md) | 토스쇼핑 As-Is 업무 카탈로그 |
| [docs/transition.md](docs/transition.md) | As-Is → To-Be application × 업무 매트릭스 |

## 참고

- Chris Richardson, *Microservices Patterns* (2018) — Outbox / Saga / Idempotent Consumer
- Martin Fowler — *MonolithFirst*, *Event-Carried State Transfer*
- Sam Newman, *Building Microservices* (2판) — Shared Database 안티패턴 / 단계적 분리
