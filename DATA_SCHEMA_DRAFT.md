# 데이터 스키마 초안
버전: 1.0
상태: MVP 기준 확정 초안

## 문서 목적
이 문서는 1차 MVP에서 필요한 핵심 데이터 구조만 정의한다.  
세부 구현은 실제 DB 종류에 맞게 조정한다.

목표는 하나다.  
**1차 MVP에 필요한 최소 구조만 먼저 확정하는 것.**

---

## 1. 핵심 원칙

1. 1차는 MVP 핵심 테이블만 우선 구현한다.
2. 과도한 선행 테이블 추가를 하지 않는다.
3. 비교 기준은 기본적으로 `도매처 + 상품코드`를 사용한다.
4. 민감정보 평문 저장을 금지한다.
5. 이 문서는 구조 정의만 담당하며 구현 상세는 포함하지 않는다.
6. 나중에 확장 가능해야 하지만, 1차에 과도한 일반화는 하지 않는다.

---

## 2. 1차 우선 구현 테이블

1. users
2. wholesalers
3. collection_runs
4. normalized_products
5. baseline_files
6. compare_runs
7. compare_results
8. export_formats
9. export_runs

아래 테이블은 필요 시 1차 후반 또는 2차로 확장한다.

- collection_run_logs
- raw_products
- baseline_rows
- system_settings
- product_snapshots
- wholesaler_credentials

---

## 3. users

### 목적
관리자/직원 계정 관리

### 주요 컬럼
- id
- username
- password_hash
- role
- is_active
- created_at
- updated_at
- last_login_at

### role
- admin
- staff

---

## 4. wholesalers

### 목적
도매처 기본 정보 관리

### 주요 컬럼
- id
- code
- name
- site_url
- is_active
- login_required
- notes
- created_at
- updated_at

### 예시 code
- ownerclan
- domaetopia
- onchannel

---

## 5. collection_runs

### 목적
수집 실행 이력 저장

### 주요 컬럼
- id
- wholesaler_id
- run_type
- trigger_type
- status
- started_at
- finished_at
- total_items
- total_pages
- success_count
- fail_count
- error_summary
- created_by_user_id
- created_at

### run_type
- full
- partial
- manual_test

### trigger_type
- manual
- scheduled
- system

### status
- pending
- running
- success
- failed
- partial_success

---

## 6. normalized_products

### 목적
정규화된 최신 상품 데이터 저장

### 주요 컬럼
- id
- wholesaler_id
- collection_run_id
- source_product_code
- unique_product_key
- product_name
- option_name
- price
- supply_price
- stock_qty
- status
- image_url
- detail_url
- category_name
- brand_name
- manufacturer_name
- is_active
- raw_hash
- collected_at
- created_at
- updated_at

### unique_product_key
기본적으로 아래 기준을 사용한다.

- `wholesaler_code + source_product_code`
또는
- `wholesaler_id + source_product_code`

### status
- active
- out_of_stock
- discontinued
- unknown

---

## 7. baseline_files

### 목적
비교 기준 파일 메타데이터 저장

### 주요 컬럼
- id
- original_filename
- stored_path
- file_hash
- uploaded_by_user_id
- uploaded_at
- notes

---

## 8. compare_runs

### 목적
비교 실행 이력 저장

### 주요 컬럼
- id
- baseline_file_id
- wholesaler_id
- status
- total_items
- new_count
- changed_count
- out_of_stock_count
- discontinued_count
- restocked_count
- unchanged_count
- started_at
- finished_at
- created_by_user_id
- created_at
- error_summary

### status
- pending
- running
- success
- failed
- partial_success

---

## 9. compare_results

### 목적
비교 결과 저장

### 주요 컬럼
- id
- compare_run_id
- wholesaler_id
- unique_product_key
- compare_status
- before_data_json
- after_data_json
- diff_json
- created_at

### compare_status
- new
- changed
- out_of_stock
- discontinued
- restocked
- unchanged

---

## 10. export_formats

### 목적
출력 포맷 정의 관리

### 주요 컬럼
- id
- code
- name
- description
- is_active
- created_at
- updated_at

### 예시 code
- master_default
- compare_default
- custom_basic_01

---

## 11. export_runs

### 목적
다운로드 생성 이력 저장

### 주요 컬럼
- id
- export_format_id
- source_type
- source_ref_id
- generated_by_user_id
- output_path
- output_filename
- status
- created_at
- error_summary

### source_type
- products
- compare_results
- custom

### status
- pending
- running
- success
- failed

---

## 12. 조건부 확장 테이블

### collection_run_logs
실행 상세 로그가 필요할 때 사용

### raw_products
원본 payload 보존이 필요할 때 사용

### baseline_rows
기준 파일 행 단위 저장이 필요할 때 사용

### system_settings
운영 설정을 DB 저장으로 옮길 때 사용

### product_snapshots
상품 상태 이력 추적이 필요할 때 사용

### wholesaler_credentials
도매처 계정 정보를 안전하게 저장해야 할 때 사용  
단, 평문 저장 금지

---

## 13. 1차 핵심 관계

- users 1:N collection_runs
- users 1:N baseline_files
- users 1:N compare_runs
- users 1:N export_runs

- wholesalers 1:N collection_runs
- wholesalers 1:N normalized_products
- wholesalers 1:N compare_runs
- wholesalers 1:N compare_results

- baseline_files 1:N compare_runs
- compare_runs 1:N compare_results

- export_formats 1:N export_runs

---

## 14. 상태 정의

### 상품 상태
- active
- out_of_stock
- discontinued
- unknown

### 비교 상태
- new
- changed
- out_of_stock
- discontinued
- restocked
- unchanged

### 실행 상태
- pending
- running
- success
- failed
- partial_success

---

## 15. 스키마 운영 원칙

1. 1차는 핵심 컬럼만 구현한다.
2. 후속 단계에서 필요한 컬럼만 추가한다.
3. 자주 바뀌는 규칙은 스키마보다 설정 계층으로 분리한다.
4. 과도한 선행 일반화는 하지 않는다.
5. 시크릿 정보는 이 스키마에 평문으로 저장하지 않는다.