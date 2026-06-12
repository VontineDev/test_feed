# 코스닥 종목기본정보

## 개요

- **설명**: 코스닥 종목기본정보 제공
- **데이터 제공 시작**: 2010년 01월 04일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/sto/ksq_isu_base_info`
- **HTTP Method**: GET

## Request Parameters (InBlock_1)

| Name | Type | Description |
|------|------|-------------|
| `basDd` | string | 기준일자 |

## Response Fields (OutBlock_1)

| Name | Type | Description |
|------|------|-------------|
| `ISU_CD` | string | 표준코드 |
| `ISU_SRT_CD` | string | 단축코드 |
| `ISU_NM` | string | 한글 종목명 |
| `ISU_ABBRV` | string | 한글 종목약명 |
| `ISU_ENG_NM` | string | 영문 종목명 |
| `LIST_DD` | string | 상장일 |
| `MKT_TP_NM` | string | 시장구분 |
| `SECUGRP_NM` | string | 증권구분 |
| `SECT_TP_NM` | string | 소속부 |
| `KIND_STKCERT_TP_NM` | string | 주식종류 |
| `PARVAL` | string | 액면가 |
| `LIST_SHRS` | string | 상장주식수 |

## Request Sample

```json
{"basDd":"20230102"}
```

## Response Sample

```json
{"OutBlock_1":[{"ISU_CD":"KR7247540005","ISU_SRT_CD":"247540","ISU_NM":"에코프로비엠","ISU_ABBRV":"에코프로비엠","ISU_ENG_NM":"EcoPro BM Co., Ltd.","LIST_DD":"20190219","MKT_TP_NM":"KOSDAQ","SECUGRP_NM":"주권","SECT_TP_NM":"일반기업부","KIND_STKCERT_TP_NM":"보통주","PARVAL":"500","LIST_SHRS":"75000000"}]}
```