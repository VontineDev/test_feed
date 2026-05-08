# 석유시장 일별매매정보

## 개요

- **설명**: KRX 석유시장의 매매정보 제공
- **데이터 제공 시작**: 2012년 03월 30일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/gen/oil_bydd_trd`
- **HTTP Method**: GET

## Request Parameters (InBlock_1)

| Name | Type | Description |
|------|------|-------------|
| `basDd` | string | 기준일자 |

## Response Fields (OutBlock_1)

| Name | Type | Description |
|------|------|-------------|
| `BAS_DD` | string | 기준일자 |
| `OIL_NM` | string | 유종구분 |
| `WT_AVG_PRC` | string | 가중평균가격_경쟁 |
| `WT_DIS_AVG_PRC` | string | 가중평균가격_협의 |
| `ACC_TRDVOL` | string | 거래량 |
| `ACC_TRDVAL` | string | 거래대금 |

## Request Sample

```json
{"basDd":"20230102"}
```

## Response Sample

```json
{"OutBlock_1":[{"BAS_DD":"20230102","OIL_NM":"휘발유","WT_AVG_PRC":"1650.00","WT_DIS_AVG_PRC":"1640.00","ACC_TRDVOL":"5000","ACC_TRDVAL":"8250000000"}]}
```