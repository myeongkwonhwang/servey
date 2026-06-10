# ADR-0001 — SAGA 스타일 (Choreography vs Orchestration)

- **Status**: Proposed (아직 결정 전, 다듬는 중)
- **Date**: 2026-06-05
- **Deciders**: 황명권
- **Related**: [README](../../README.md)

---

## Context

`partner-channel-msa` 는 토스쇼핑 채널의 주문/클레임 흐름을 마이크로서비스로 분리하면서, 여러 서비스에 걸친 트랜잭션 일관성을 유지해야 한다.

선택지:
- **XA / 2PC** — 토스 외부 API + Kafka 는 XA 비참여. 사실상 적용 불가.
- **SAGA (보상 트랜잭션)** — 채택. 단, **코레오그래피 vs 오케스트레이션** 결정 필요.

토스 클레임 흐름의 특징:
- 단계가 비교적 정해져 있음 (수신 → 검증 → 토스 confirm/deny API → 사내 DB 반영 → outbox 발행 → 후속 알림/통계)
- **보상 트리거가 명확** — REVOKED, API 실패, DB 실패 등
- 채널이 토스 하나로 시작하지만, 추후 다른 채널 확장 시 패턴 재사용이 목표

## Options

### Option A — Choreography (코레오그래피)

각 서비스가 도메인 이벤트를 발행하고 다른 서비스가 그 이벤트를 구독해 다음 단계를 진행. 중앙 코디네이터 없음.

**장점**
- 결합도 매우 낮음 — 서비스 간 직접 호출 없음
- 새 step 추가 시 새 구독자만 붙이면 됨
- SPOF 없음

**단점**
- 흐름 전체 파악 어려움 — 코드만 봐서는 순서가 안 보임
- 디버깅 시 Distributed Tracing 필수 (OpenTelemetry 등)
- 순환 의존 / 이벤트 폭포 위험
- 보상 트랜잭션 트리거가 분산되어 일관 관리 어려움

### Option B — Orchestration (오케스트레이션) ★ 추천

중앙 SAGA Orchestrator 가 상태머신을 가지고 각 단계의 서비스를 호출. 보상 트리거도 코디네이터가 수행.

**장점**
- 흐름이 한 곳에 명시 — 운영/디버깅 용이
- 보상 트랜잭션이 한 코드에서 추적됨
- 상태 시각화/감사 추적 자연스러움
- 토스 클레임처럼 step 이 정해진 흐름에 적합

**단점**
- 코디네이터에 결합도 발생 (orchestrator ↔ 각 서비스)
- 코디네이터 자체가 SPOF → HA 구성 필요
- 새 step 추가 시 코디네이터 수정 필요

### Option C — Hybrid

핵심 흐름(주문 수신, 클레임 처리, 송장 등록)은 오케스트레이션, 후행성 흐름(알림, 통계, 감사 로그)은 코레오그래피.

- 핵심 흐름의 운영성/추적성 확보
- 가벼운 부가 흐름은 결합 없이 확장

## Decision (안 — 다듬는 중)

**Option B (Orchestration) 을 메인으로, Option C 의 Hybrid 방향까지 열어둔다.**

근거:
1. 토스 클레임 흐름은 step 시퀀스가 명확하고 보상 케이스([[project_tss_saga_assessment]] 의 REVOKED 등) 가 식별되어 있음 → 코디네이터에 집약하는 편이 유지보수성 ↑
2. 학습/실험 목적이 있는 프로젝트 — 오케 부터 시작해 동작이 명확해진 뒤 일부 흐름을 코레오로 분리해보는 학습 경로가 자연스러움
3. 후행 알림/통계 등은 처음부터 코레오 (이벤트 발행만) 로 두면 양쪽 패턴을 한 프로젝트에서 모두 다뤄볼 수 있음

## SAGA 구현 방식 (별도 결정 항목)

오케 채택 시 구현 방식 후보 — 별도 ADR-0001a 로 분리해 다음 단계에서 결정.

| 방식 | 비고 |
|------|------|
| 자체 구현 (Outbox + State Machine) | 의존성 없음, 학습 가치 ↑, 운영 부담 ↑ |
| Spring Statemachine | Spring 친화적, 학습 곡선 중간 |
| Axon Framework | CQRS/Event Sourcing 통합, 학습량 ↑ |
| Eventuate Tram Saga | SAGA 전용 라이브러리 (Chris Richardson) |
| Temporal / Cadence | 워크플로 엔진. 별도 인프라 필요 |
| Camunda 8 (Zeebe) | BPMN 기반, 비개발자 가시성 ↑, 인프라 필요 |

**추천 진행 순서**: 자체 구현 → 동작 잡힌 뒤 라이브러리(Eventuate Tram 또는 Temporal) 적용 비교.

## Consequences

채택 시:
- (+) 흐름 가시성 / 보상 추적성 확보
- (+) 운영/디버깅 진입장벽 낮음
- (−) 코디네이터 서비스 HA 구성 필요 (Kafka offset, 상태 영속화)
- (−) 새 step 추가 시 코디네이터 수정 부담
- (−) Hybrid 채택 시 두 패턴이 한 시스템에 공존 — 일관 가이드라인 필요

## Open Questions

다음 라운드에서 같이 다듬을 항목:
1. 오케 vs 하이브리드 — 처음부터 하이브리드로 갈지, 오케 일관으로 시작 후 확장할지?
2. SAGA 구현 방식 6개 중 시작점? (자체 구현 추천이나 의견 차이 환영)
3. 코디네이터 자체를 하나의 서비스로 둘지, 도메인 서비스 안에 내장할지?
4. 보상 트랜잭션의 멱등성 보장 방식 — Idempotency-Key 테이블 vs Outbox 의 dedup?

## References

- [README](../../README.md)
- 메모리: `project_tss_saga_assessment.md` — 현 토스 SAGA 평가 결과
- 메모리: `project_tss_revoked_compensation_decision.md` — REVOKED 보상 검토
- 메모리: `reference_toss_claim_flow.md` — 토스 클레임 API 흐름
- Chris Richardson, *Microservices Patterns* (2018) Ch.4 (SAGA Pattern)
