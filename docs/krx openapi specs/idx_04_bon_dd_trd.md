# 채권지수 시세정보

## 개요

- **설명**: 채권지수의 시세정보 제공
- **데이터 제공 시작**: 2010년 01월 04일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/idx/bon_dd_trd`
- **HTTP Method**: GET

## Request Parameters (InBlock_1)

| Name | Type | Description |
|------|------|-------------|
| `basDd` | string | 기준일자 |

## Response Fields (OutBlock_1)

| Name | Type | Description |
|------|------|-------------|
| `BAS_DD` | string | 기준일자 |
| `BND_IDX_GRP_NM` | string | 지수명 |
| `TOT_EARNG_IDX` | string | 총수익지수_종가 |
| `TOT_EARNG_IDX_CMPPREVDD` | string | 총수익지수_대비 |
| `NETPRC_IDX` | string | 순가격지수_종가 |
| `NETPRC_IDX_CMPPREVDD` | string | 순가격지수_대비 |
| `ZERO_REINVST_IDX` | string | 제로재투자지수_종가 |
| `ZERO_REINVST_IDX_CMPPREVDD` | string | 제로재투자지수_대비 |
| `CALL_REINVST_IDX` | string | 콜재투자지수_종가 |
| `CALL_REINVST_IDX_CMPPREVDD` | string | 콜재투자지수_대비 |
| `MKT_PRC_IDX` | string | 시장가격지수_종가 |
| `MKT_PRC_IDX_CMPPREVDD` | string | 시장가격지수_대비 |
| `AVG_DURATION` | string | 듀레이션 |
| `AVG_CONVEXITY_PRC` | string | 컨벡시티 |
| `BND_IDX_AVG_YD` | string | YTM |

## Request Sample

```json
{"basDd":"20230102"}
```

## Response Sample

```json
{"OutBlock_1":[{"BAS_DD":"20230102","BND_IDX_GRP_NM":"KRX 채권종합","TOT_EARNG_IDX":"5000.00","TOT_EARNG_IDX_CMPPREVDD":"1.20","NETPRC_IDX":"1100.00","NETPRC_IDX_CMPPREVDD":"0.50","ZERO_REINVST_IDX":"1050.00","ZERO_REINVST_IDX_CMPPREVDD":"0.30","CALL_REINVST_IDX":"1060.00","CALL_REINVST_IDX_CMPPREVDD":"0.40","MKT_PRC_IDX":"1080.00","MKT_PRC_IDX_CMPPREVDD":"0.45","AVG_DURATION":"3.50","AVG_CONVEXITY_PRC":"15.20","BND_IDX_AVG_YD":"3.80"}]}
```