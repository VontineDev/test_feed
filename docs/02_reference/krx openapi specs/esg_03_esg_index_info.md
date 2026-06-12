# ESG 지수 정보

## 개요

- **설명**: ESG 지수 정보를 제공
- **데이터 제공 시작**: 2020년 01월 02일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/esg/esg_index_info`
- **HTTP Method**: GET

## Request Parameters (InBlock_1)

| Name | Type | Description |
|------|------|-------------|
| `basDd` | string | 기준일자 |

## Response Fields (OutBlock_1)

| Name | Type | Description |
|------|------|-------------|
| `BAS_DD` | string | 기준일자 |
| `IDX_NM` | string | 지수명 |
| `CLSPRC_IDX` | string | 현재가 |
| `PRV_DD_CMPR` | string | 전일비 |
| `UPDN_RATE` | string | 등락률 |
| `TRD_ISU_CNT` | string | 구성종목수 |
| `ACC_TRDVOL` | string | 거래량(천주) |
| `ACC_TRDVAL` | string | 거래대금(백만원) |

## Request Sample

```json
{"basDd":"20230102"}
```

## Response Sample

```json
{"OutBlock_1":[{"BAS_DD":"20230102","IDX_NM":"KRX ESG Leaders 150","CLSPRC_IDX":"1500.00","PRV_DD_CMPR":"10.00","UPDN_RATE":"0.67","TRD_ISU_CNT":"150","ACC_TRDVOL":"500000","ACC_TRDVAL":"30000"}]}
```