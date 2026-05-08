# 선물 일별매매정보 (주식선물 外)

## 개요

- **설명**: 파생상품시장의 선물 중 주식선물을 제외한 선물의 매매정보 제공
- **데이터 제공 시작**: 2010년 01월 04일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/drv/fut_bydd_trd`
- **HTTP Method**: GET

## Request Parameters (InBlock_1)

| Name | Type | Description |
|------|------|-------------|
| `basDd` | string | 기준일자 |

## Response Fields (OutBlock_1)

| Name | Type | Description |
|------|------|-------------|
| `BAS_DD` | string | 기준일자 |
| `PROD_NM` | string | 상품구분 |
| `MKT_NM` | string | 시장구분(정규/야간) |
| `ISU_CD` | string | 종목코드 |
| `ISU_NM` | string | 종목명 |
| `TDD_CLSPRC` | string | 종가 |
| `CMPPREVDD_PRC` | string | 대비 |
| `TDD_OPNPRC` | string | 시가 |
| `TDD_HGPRC` | string | 고가 |
| `TDD_LWPRC` | string | 저가 |
| `SPOT_PRC` | string | 현물가 |
| `SETL_PRC` | string | 정산가 |
| `ACC_TRDVOL` | string | 거래량 |
| `ACC_TRDVAL` | string | 거래대금 |
| `ACC_OPNINT_QTY` | string | 미결제약정 |

## Request Sample

```json
{"basDd":"20230102"}
```

## Response Sample

```json
{"OutBlock_1":[{"BAS_DD":"20230102","PROD_NM":"KOSPI200선물","MKT_NM":"정규","ISU_CD":"101W3000","ISU_NM":"KOSPI200 2303","TDD_CLSPRC":"349.90","CMPPREVDD_PRC":"2.50","TDD_OPNPRC":"347.40","TDD_HGPRC":"350.20","TDD_LWPRC":"347.00","SPOT_PRC":"350.25","SETL_PRC":"349.90","ACC_TRDVOL":"250000","ACC_TRDVAL":"4373750000000","ACC_OPNINT_QTY":"120000"}]}
```