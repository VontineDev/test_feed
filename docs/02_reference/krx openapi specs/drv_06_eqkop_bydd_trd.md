# 주식옵션(코스닥) 일별매매정보

## 개요

- **설명**: 파생상품시장의 주식옵션 중 기초자산이 코스닥시장에 속하는 주식옵션의 거래정보 제공
- **데이터 제공 시작**: 2017년 06월 26일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/drv/eqkop_bydd_trd`
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
{"OutBlock_1":[{"BAS_DD":"20230102","PROD_NM":"코스닥주식옵션","RGHT_TP_NM":"PUT","ISU_CD":"KQ8247540P","ISU_NM":"에코프로비엠P 230316 200000","TDD_CLSPRC":"3000","CMPPREVDD_PRC":"-200","TDD_OPNPRC":"3200","TDD_HGPRC":"3300","TDD_LWPRC":"2900","IMP_VOLT":"30.00","NXTDD_BAS_PRC":"2980","ACC_TRDVOL":"200","ACC_TRDVAL":"600000000","ACC_OPNINT_QTY":"1000"}]}
```