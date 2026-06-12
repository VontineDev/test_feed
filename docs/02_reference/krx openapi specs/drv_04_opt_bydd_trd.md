# 옵션 일별매매정보 (주식옵션 外)

## 개요

- **설명**: 파생상품시장의 옵션 중 주식옵션을 제외한 옵션의 매매정보 제공
- **데이터 제공 시작**: 2010년 01월 04일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/drv/opt_bydd_trd`
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
| `RGHT_TP_NM` | string | 권리유형(CALL/PUT) |
| `ISU_CD` | string | 종목코드 |
| `ISU_NM` | string | 종목명 |
| `TDD_CLSPRC` | string | 종가 |
| `CMPPREVDD_PRC` | string | 대비 |
| `TDD_OPNPRC` | string | 시가 |
| `TDD_HGPRC` | string | 고가 |
| `TDD_LWPRC` | string | 저가 |
| `IMP_VOLT` | string | 내재변동성 |
| `NXTDD_BAS_PRC` | string | 익일정산가 |
| `ACC_TRDVOL` | string | 거래량 |
| `ACC_TRDVAL` | string | 거래대금 |
| `ACC_OPNINT_QTY` | string | 미결제약정 |

## Request Sample

```json
{"basDd":"20230102"}
```

## Response Sample

```json
{"OutBlock_1":[{"BAS_DD":"20230102","PROD_NM":"KOSPI200옵션","RGHT_TP_NM":"CALL","ISU_CD":"201W3350","ISU_NM":"KOSPI200C 230316 350","TDD_CLSPRC":"2.50","CMPPREVDD_PRC":"0.30","TDD_OPNPRC":"2.20","TDD_HGPRC":"2.60","TDD_LWPRC":"2.10","IMP_VOLT":"18.50","NXTDD_BAS_PRC":"2.48","ACC_TRDVOL":"50000","ACC_TRDVAL":"12500000000","ACC_OPNINT_QTY":"80000"}]}
```