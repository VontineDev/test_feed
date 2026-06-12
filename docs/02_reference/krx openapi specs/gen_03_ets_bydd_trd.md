# 배출권 시장 일별매매정보

## 개요

- **설명**: KRX 탄소배출권 시장의 매매정보 제공
- **데이터 제공 시작**: 2015년 01월 12일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/gen/ets_bydd_trd`
- **HTTP Method**: GET

## Request Parameters (InBlock_1)

| Name | Type | Description |
|------|------|-------------|
| `basDd` | string | 기준일자 |

## Response Fields (OutBlock_1)

| Name | Type | Description |
|------|------|-------------|
| `BAS_DD` | string | 기준일자 |
| `ISU_CD` | string | 종목코드 |
| `ISU_NM` | string | 종목명 |
| `TDD_CLSPRC` | string | 종가 |
| `CMPPREVDD_PRC` | string | 대비 |
| `FLUC_RT` | string | 등락률 |
| `TDD_OPNPRC` | string | 시가 |
| `TDD_HGPRC` | string | 고가 |
| `TDD_LWPRC` | string | 저가 |
| `ACC_TRDVOL` | string | 거래량 |
| `ACC_TRDVAL` | string | 거래대금 |

## Request Sample

```json
{"basDd":"20230102"}
```

## Response Sample

```json
{"OutBlock_1":[{"BAS_DD":"20230102","ISU_CD":"KAU23","ISU_NM":"배출권KAU23","TDD_CLSPRC":"12000","CMPPREVDD_PRC":"200","FLUC_RT":"1.69","TDD_OPNPRC":"11800","TDD_HGPRC":"12100","TDD_LWPRC":"11750","ACC_TRDVOL":"10000","ACC_TRDVAL":"120000000000"}]}
```