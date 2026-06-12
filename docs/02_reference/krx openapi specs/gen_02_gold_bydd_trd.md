# 금시장 일별매매정보

## 개요

- **설명**: KRX 금시장 매매정보 제공
- **데이터 제공 시작**: 2014년 03월 24일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/gen/gold_bydd_trd`
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
{"OutBlock_1":[{"BAS_DD":"20230102","ISU_CD":"GC0001","ISU_NM":"금 현물","TDD_CLSPRC":"80000","CMPPREVDD_PRC":"500","FLUC_RT":"0.63","TDD_OPNPRC":"79500","TDD_HGPRC":"80200","TDD_LWPRC":"79400","ACC_TRDVOL":"100","ACC_TRDVAL":"8000000000"}]}
```