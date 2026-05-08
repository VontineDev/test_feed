# 주식선물(코스닥) 일별매매정보

## 개요

- **설명**: 파생상품시장의 주식선물 중 기초자산이 코스닥시장에 속하는 주식선물의 거래정보 제공
- **데이터 제공 시작**: 2015년 08월 03일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/drv/eqkfu_ksq_bydd_trd`
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
{"OutBlock_1":[{"BAS_DD":"20230102","PROD_NM":"코스닥주식선물","MKT_NM":"정규","ISU_CD":"KQ6247540","ISU_NM":"에코프로비엠 2303","TDD_CLSPRC":"200000","CMPPREVDD_PRC":"5000","TDD_OPNPRC":"196000","TDD_HGPRC":"202000","TDD_LWPRC":"195000","SPOT_PRC":"200000","SETL_PRC":"200000","ACC_TRDVOL":"500","ACC_TRDVAL":"1000000000","ACC_OPNINT_QTY":"2000"}]}
```