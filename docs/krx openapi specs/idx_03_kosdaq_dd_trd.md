# KOSDAQ 시리즈 일별시세정보

## 개요

- **설명**: KOSDAQ 시리즈 지수의 시세정보 제공
- **데이터 제공 시작**: 2010년 01월 04일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/idx/kosdaq_dd_trd`
- **HTTP Method**: GET

## Request Parameters (InBlock_1)

| Name | Type | Description |
|------|------|-------------|
| `basDd` | string | 기준일자 |

## Response Fields (OutBlock_1)

| Name | Type | Description |
|------|------|-------------|
| `BAS_DD` | string | 기준일자 |
| `IDX_CLSS` | string | 계열구분 |
| `IDX_NM` | string | 지수명 |
| `CLSPRC_IDX` | string | 종가 |
| `CMPPREVDD_IDX` | string | 대비 |
| `FLUC_RT` | string | 등락률 |
| `OPNPRC_IDX` | string | 시가 |
| `HGPRC_IDX` | string | 고가 |
| `LWPRC_IDX` | string | 저가 |
| `ACC_TRDVOL` | string | 거래량 |
| `ACC_TRDVAL` | string | 거래대금 |
| `MKTCAP` | string | 상장시가총액 |

## Request Sample

```json
{"basDd":"20230102"}
```

## Response Sample

```json
{"OutBlock_1":[{"BAS_DD":"20230102","IDX_CLSS":"KOSDAQ","IDX_NM":"KOSDAQ","CLSPRC_IDX":"750.00","CMPPREVDD_IDX":"2.50","FLUC_RT":"0.33","OPNPRC_IDX":"748.00","HGPRC_IDX":"755.00","LWPRC_IDX":"745.00","ACC_TRDVOL":"800000000","ACC_TRDVAL":"5000000000","MKTCAP":"400000000000"}]}
```