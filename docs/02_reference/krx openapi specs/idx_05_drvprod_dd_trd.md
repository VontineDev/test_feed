# 파생상품지수 시세정보

## 개요

- **설명**: 파생상품지수의 시세정보 제공
- **데이터 제공 시작**: 2010년 01월 04일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/idx/drvprod_dd_trd`
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

## Request Sample

```json
{"basDd":"20230102"}
```

## Response Sample

```json
{"OutBlock_1":[{"BAS_DD":"20230102","IDX_CLSS":"파생","IDX_NM":"KOSPI200 변동성지수","CLSPRC_IDX":"18.50","CMPPREVDD_IDX":"-0.20","FLUC_RT":"-1.07","OPNPRC_IDX":"18.80","HGPRC_IDX":"19.00","LWPRC_IDX":"18.30"}]}
```