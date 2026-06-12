# ESG 증권상품 정보

## 개요

- **설명**: ESG 증권상품 정보를 제공
- **데이터 제공 시작**: 2020년 01월 02일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/esg/esg_etp_info`
- **HTTP Method**: GET

## Request Parameters (InBlock_1)

| Name | Type | Description |
|------|------|-------------|
| `basDd` | string | 기준일자 |

## Response Fields (OutBlock_1)

| Name | Type | Description |
|------|------|-------------|
| `BAS_DD` | string | 기준일자 |
| `ISU_ABBRV` | string | 종목명 |
| `TDD_CLSPRC` | string | 현재가 |
| `CMPPREVDD_PRC` | string | 전일비 |
| `FLUC_RT` | string | 등락률 |
| `LIST_SHRS` | string | 상장좌수 |
| `ACC_TRDVOL` | string | 거래량(좌) |
| `ACC_TRDVAL` | string | 거래대금(원) |

## Request Sample

```json
{"basDd":"20230102"}
```

## Response Sample

```json
{"OutBlock_1":[{"BAS_DD":"20230102","ISU_ABBRV":"KODEX ESG Leaders150","TDD_CLSPRC":"12000","CMPPREVDD_PRC":"100","FLUC_RT":"0.84","LIST_SHRS":"5000000","ACC_TRDVOL":"100000","ACC_TRDVAL":"1200000000"}]}
```