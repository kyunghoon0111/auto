# 지표 & KPI 레퍼런스

> SCM 운영 분석 DB의 모든 KPI에 대한 산식, 입도, 해석 방법.

---

## 재고 지표

### `sellable_qty` (판매가능 수량)

| 항목 | 내용 |
|---|---|
| **정의** | 신규 주문에 대해 출고 가능한 수량 (ATP). |
| **산식** | `on_hand_qty - reserved_qty - damaged_qty - expired_qty` |
| **입도** | 품목 x 창고 x 일자 |
| **소스 테이블** | `core.fact_inventory_snapshot` |
| **마트 테이블** | `mart.inventory_daily` |
| **단위** | 재고 관리 단위 (EA / CS 등 품목 UOM에 따름) |

### `expired_qty` (만료 수량)

| 항목 | 내용 |
|---|---|
| **정의** | 유통기한이 지나 더 이상 판매할 수 없는 수량. |
| **산식** | `SUM(expired_qty) WHERE expiry_date < snapshot_date` |
| **입도** | 품목 x 창고 x 일자 |
| **소스 테이블** | `core.fact_inventory_snapshot` |
| **마트 테이블** | `mart.inventory_daily`, `mart.inventory_fefo` |
| **단위** | 재고 관리 단위 |

### `DOH` (재고일수)

| 항목 | 내용 |
|---|---|
| **정의** | 현재 판매가능 재고가 평균 일일 수요를 몇 일 커버할 수 있는지. |
| **산식** | `sellable_qty / AVG(daily_shipped_qty, 최근 30일)` |
| **입도** | 품목 x 창고 |
| **소스 테이블** | `core.fact_inventory_snapshot`, `core.fact_shipment` |
| **마트 테이블** | `mart.inventory_coverage` |
| **단위** | 일 |
| **예외 처리** | `daily_shipped_qty = 0` (수요 없음)이면 DOH = `999` (상한). |

### `stockout` (품절, 이진값)

| 항목 | 내용 |
|---|---|
| **정의** | 활성 수요가 있는데 판매가능 재고가 0인 상태. |
| **산식** | `sellable_qty = 0 AND EXISTS open_orders_for_item` |
| **입도** | 품목 x 창고 x 일자 |
| **마트 테이블** | `mart.inventory_coverage` |
| **단위** | 불리언 플래그 (1/0) |

### `days_of_cover` (재고 커버 일수)

| 항목 | 내용 |
|---|---|
| **정의** | 입고 예정 발주를 포함한 전방 커버 일수. |
| **산식** | `(sellable_qty + pending_po_qty) / AVG(daily_shipped_qty, 최근 30일)` |
| **입도** | 품목 x 창고 |
| **소스 테이블** | `core.fact_inventory_snapshot`, `core.fact_po`, `core.fact_shipment` |
| **마트 테이블** | `mart.inventory_coverage` |
| **단위** | 일 |
| **예외 처리** | 수요 없을 때 `999` 상한. 입고 예정 PO는 상태가 OPEN 또는 PARTIAL인 것만 포함. |

### `FEFO 순위`

| 항목 | 내용 |
|---|---|
| **정의** | 선입선출(FEFO: First-Expired-First-Out) 기준 로트 소진 우선순위. |
| **산식** | `ROW_NUMBER() OVER (PARTITION BY item_id, warehouse_id ORDER BY expiry_date ASC NULLS LAST, lot_id ASC)` |
| **입도** | 품목 x 창고 x 로트 |
| **마트 테이블** | `mart.inventory_fefo` |
| **단위** | 순위 (1 = 가장 먼저 소진) |

---

## 서비스 & 이행 지표

### `service_level_pct` (서비스 수준)

| 항목 | 내용 |
|---|---|
| **정의** | 약속 출고일 이내에 출고된 주문 라인의 비율. |
| **산식** | `COUNT(actual_ship_date <= promised_ship_date) / COUNT(shipped_lines) * 100` |
| **입도** | 창고 x 기간 |
| **소스 테이블** | `core.fact_order`, `core.fact_shipment` |
| **마트 테이블** | `mart.shipment_performance` |
| **단위** | 퍼센트 (%) |
| **목표** | >= 95% |

### `fulfillment_rate` (이행률)

| 항목 | 내용 |
|---|---|
| **정의** | 주문 수량 대비 실제 출고된 수량의 비율. |
| **산식** | `SUM(shipped_qty) / SUM(ordered_qty) * 100` |
| **입도** | 품목 x 채널 x 기간 |
| **소스 테이블** | `core.fact_order`, `core.fact_shipment` |
| **마트 테이블** | `mart.order_item_performance` |
| **단위** | 퍼센트 (%) |
| **목표** | >= 98% |

---

## 구매/조달 지표

### `late_po_ratio` (발주 지연율)

| 항목 | 내용 |
|---|---|
| **정의** | 납기 예정일 이후에 입고된 발주 라인의 비율. |
| **산식** | `COUNT(receipt_date > expected_delivery_date) / COUNT(received_po_lines) * 100` |
| **입도** | 거래처 (공급업체) x 기간 |
| **소스 테이블** | `core.fact_po`, `core.fact_receipt` |
| **마트 테이블** | `mart.po_tracking` |
| **단위** | 퍼센트 (%) |
| **목표** | <= 10% |

---

## 재무 지표

### `gross_margin_pct` (매출총이익률)

| 항목 | 내용 |
|---|---|
| **정의** | 매출 대비 매출총이익의 비율. |
| **산식** | `(SUM(unit_price * shipped_qty) - SUM(total_landed_cost)) / SUM(unit_price * shipped_qty) * 100` |
| **입도** | 품목 x 채널 x 기간 |
| **소스 테이블** | `core.fact_order`, `core.fact_shipment`, `core.fact_cost_structure` |
| **마트 테이블** | `mart.margin_analysis` |
| **단위** | 퍼센트 (%) |
| **비고** | `total_landed_cost` = 전 비용 단계의 합계 (INBOUND + STORAGE + OUTBOUND + RETURN + CUSTOMS). |

### `contribution_pct` (공헌이익률)

| 항목 | 내용 |
|---|---|
| **정의** | 카테고리 내 총 매출총이익에서 해당 품목이 차지하는 비중. |
| **산식** | `item_gross_margin / SUM(gross_margin) OVER category_l1 * 100` |
| **입도** | 품목 x 기간 |
| **마트 테이블** | `mart.margin_analysis` |
| **단위** | 퍼센트 (%) |

---

## 커버리지 & 도메인 지표

### `coverage_rate` (커버리지율)

| 항목 | 내용 |
|---|---|
| **정의** | 해당 기간에 데이터가 존재하는 REQUIRED 도메인 테이블의 비율. |
| **산식** | `COUNT(tables_with_rows) / COUNT(required_tables) * 100` |
| **입도** | 기간 |
| **소스** | `ops.ops_period_close`, 도메인 커버리지 설정 |
| **마트 테이블** | 마감 시 산출; `ops.ops_period_close.notes`에 저장 |
| **단위** | 퍼센트 (%) |
| **목표** | 기간 마감 시 100% |

---

## P&L 손익 지표 (신규)

### `net_revenue_krw` (순매출)

| 항목 | 내용 |
|---|---|
| **정의** | 정산 기준 순매출 (총매출 - 할인 - 환불), KRW 환산. |
| **산식** | `net_payout * fx_rate_to_krw` (KRW 통화는 rate=1.0; 비-KRW 환율 누락 시 NULL) |
| **입도** | 기간 x 품목 x 채널 x 국가 |
| **마트 테이블** | `mart.mart_pnl_revenue` |
| **coverage_flag** | FX 누락 → `PARTIAL`; KRW → `ACTUAL` |

### `gross_sales_krw` / `discounts_krw` / `refunds_krw` (매출 분해)

| 항목 | 내용 |
|---|---|
| **정의** | 총매출, 할인액, 환불액 (각각 KRW 환산). |
| **산식** | `settlement.gross_sales * fx_rate`, `settlement.discounts * fx_rate`, `settlement.refunds * fx_rate` |
| **입도** | 기간 x 품목 x 채널 x 국가 |
| **마트 테이블** | `mart.mart_pnl_revenue` |
| **비고** | 할인율 = `|discounts_krw| / |gross_sales_krw|` (대시보드에서 산출) |

### `cogs_krw` (매출원가)

| 항목 | 내용 |
|---|---|
| **정의** | 판매 출고 기준 매출원가. 원가 마스터의 as-of join으로 산출. |
| **산식** | `qty_net * unit_cost_krw` (unit_cost = `SUM(cost_per_unit_krw)` from cost_agg CTE) |
| **입도** | 기간 x 품목 x 채널 |
| **마트 테이블** | `mart.mart_pnl_cogs` |
| **coverage_flag** | 원가 누락 → `cogs_krw=NULL`, `PARTIAL` |
| **안전 규칙** | 판매 전용 (`channel_order_id IS NOT NULL`); 원가 사전 집계 (join 폭발 방지); 그레인 정렬 ROW_NUMBER |

### `operating_profit_krw` (영업이익)

| 항목 | 내용 |
|---|---|
| **정의** | 공헌이익 - 고정비. 현재 고정비 미입력 시 영업이익 = 공헌이익. |
| **산식** | `contribution_krw - fixed_cost_krw` |
| **입도** | 기간 x 품목 x 채널 |
| **마트 테이블** | `mart.mart_pnl_operating_profit` |
| **coverage_flag** | 공헌이익에서 상속 (전파 규칙) |

### `known_sum` / `total_sum_min` (커버리지 집계)

| 항목 | 내용 |
|---|---|
| **정의** | coverage_flag 기반 이중 집계. known_sum = ACTUAL 행만 합산; total_sum_min = 누락행 0 가정 최소값. |
| **산식** | `SUM(CASE WHEN coverage_flag='ACTUAL' THEN metric END)` / `SUM(COALESCE(metric, 0))` |
| **비고** | known_sum의 CASE에 ELSE 0 절 없음 (NULL 행은 합산에서 자동 제외). 대시보드 헬퍼 `_coverage_agg_sql()`에서 사용. |

---

### `inventory_value_krw` (재고금액)

| 항목 | 내용 |
|---|---|
| **정의** | 재고 수량 × 최신 단위원가. 원가 누락 품목은 금액 미산출. |
| **산식** | `onhand_qty * latest_unit_cost_krw` (cost_agg → cost_latest CTE) |
| **입도** | 품목 x 창고 |
| **마트 테이블** | 대시보드 실시간 조인 (`mart.mart_inventory_onhand` + `core.fact_cost_structure`) |
| **비고** | sellable_value, hold_value, expired_value도 동일 로직 |

### `turnover_ratio` (재고회전율)

| 항목 | 내용 |
|---|---|
| **정의** | 기간 출고수량(판매) / 평균 재고수량. |
| **산식** | `SUM(qty_shipped WHERE channel_order_id IS NOT NULL) / AVG(onhand_qty)` |
| **입도** | 품목 |
| **마트 테이블** | 대시보드 실시간 조인 |
| **비고** | 판매 전용 필터 적용; 평균 재고 = mart_inventory_onhand 기준 |

### `overstock_value_krw` (과재고 금액)

| 항목 | 내용 |
|---|---|
| **정의** | 과재고 수량 × 최신 단위원가. |
| **산식** | `overstock_qty * latest_unit_cost_krw` |
| **입도** | 품목 x 창고 |
| **마트 테이블** | 대시보드 실시간 조인 (`mart.mart_overstock` + cost_latest) |
| **비고** | 원가 미확인 품목은 금액 미산출 (NULL) |

### `expiry_risk_value_krw` (유통기한 위험금액)

| 항목 | 내용 |
|---|---|
| **정의** | 유통기한 위험 재고의 원가 기준 금액. |
| **산식** | `risk_value_krw` = `onhand_qty * unit_cost_krw` (as-of join, NULL 전파) |
| **입도** | 품목 x 창고 x 로트 |
| **마트 테이블** | `mart.mart_expiry_risk` |
| **비고** | 원가 누락 시 `risk_value_krw = NULL` (0 채우기 금지) |

---

## 지표 요약 테이블

| KPI | 입도 | 산식 (요약) | 목표 | 마트 테이블 |
|---|---|---|---|---|
| `sellable_qty` | 품목 x 창고 x 일자 | on_hand - reserved - damaged - expired | — | `mart_inventory_onhand` |
| `expired_qty` | 품목 x 창고 x 일자 | 유통기한 경과분 합계 | 0 | `mart_inventory_onhand` |
| `DOH` | 품목 x 창고 | sellable / avg_daily_demand | 30-90일 | `mart_stockout_risk` |
| `stockout` | 품목 x 창고 | sellable=0 AND 수요>0 | 0 | `mart_stockout_risk` |
| `days_of_cover` | 품목 x 창고 | (sellable + pending_po) / avg_daily_demand | > 14일 | `mart_stockout_risk` |
| `FEFO 순위` | 품목 x 창고 x 로트 | ROW_NUMBER by expiry_date ASC | — | `mart_fefo_pick_list` |
| `service_level_pct` | 주차 x 채널 | 정시 라인 / 전체 라인 | >= 95% | `mart_service_level` |
| `fulfillment_rate` | 품목 x 채널 x 기간 | shipped_qty / ordered_qty | >= 98% | `mart_shipment_performance` |
| `late_po_ratio` | 거래처 x 기간 | 지연 라인 / 전체 입고 라인 | <= 10% | `mart_open_po` |
| `gross_margin_pct` | 품목 x 채널 x 기간 | (순매출 - COGS) / 순매출 | > 0% | `mart_pnl_gross_margin` |
| `contribution_pct` | 품목 x 채널 x 기간 | (매출총이익 - 변동비) / 순매출 | — | `mart_pnl_contribution` |
| `operating_profit_pct` | 품목 x 채널 x 기간 | (공헌이익 - 고정비) / 순매출 | > 0% | `mart_pnl_operating_profit` |
| `inventory_value_krw` | 품목 x 창고 | onhand_qty × latest_unit_cost | — | 대시보드 실시간 |
| `turnover_ratio` | 품목 | 출고(판매) / 평균재고 | > 1.0 | 대시보드 실시간 |
| `overstock_value_krw` | 품목 x 창고 | overstock_qty × unit_cost | — | 대시보드 실시간 |
| `expiry_risk_value` | 품목 x 창고 x 로트 | onhand_qty × unit_cost (NULL 전파) | 0 | `mart_expiry_risk` |
| `coverage_rate` | 기간 | 데이터 있는 테이블 / 필수 테이블 | 100% | `mart_coverage_period` |
| `known_sum` | 지표별 | SUM(CASE WHEN flag='ACTUAL') — no ELSE | — | 대시보드 헬퍼 |
