# 사회책임투자채권 정보

## 개요

- **설명**: 사회책임투자채권(SRI Bond) 정보를 제공
- **데이터 제공 시작**: 2019년 01월 01일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/esg/sri_bond_info`
- **HTTP Method**: GET

## Request Parameters (InBlock_1)

| Name | Type | Description |
|------|------|-------------|
| `basDd` | string | 기준일자 |

## Response Fields (OutBlock_1)

| Name | Type | Description |
|------|------|-------------|
| `BAS_DD` | string | 기준일자 |
| `ISUR_NM` | string | 발행기관 |
| `ISU_CD` | string | 표준코드 |
| `SRI_BND_TP_NM` | string | 채권종류 |
| `ISU_NM` | string | 종목명 |
| `LIST_DD` | string | 상장일 |
| `ISU_DD` | string | 발행일 |
| `REDMPT_DD` | string | 상환일 |
| `ISU_RT` | string | 표면이자율 |
| `ISU_AMT` | string | 발행금액 |
| `LIST_AMT` | string | 상장금액 |
| `BND_TP_NM` | string | 채권유형 |

## Request Sample

```json
{"basDd":"20230102"}
```

## Response Sample

```json
{"OutBlock_1":[{"BAS_DD":"20230102","ISUR_NM":"한국산업은행","ISU_CD":"KR2001001D12","SRI_BND_TP_NM":"녹색채권","ISU_NM":"산금채23-3(녹색)","LIST_DD":"20230110","ISU_DD":"20230110","REDMPT_DD":"20260110","ISU_RT":"3.80","ISU_AMT":"100000000000","LIST_AMT":"100000000000","BND_TP_NM":"특수채"}]}
```