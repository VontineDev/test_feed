# ETF 일별매매정보

## 개요

- **설명**: ETF(상장지수펀드)의 매매정보 제공
- **데이터 제공 시작**: 2010년 01월 04일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/etp/etf_bydd_trd`
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
| `NAV` | string | 순자산가치(NAV) |
| `TDD_OPNPRC` | string | 시가 |
| `TDD_HGPRC` | string | 고가 |
| `TDD_LWPRC` | string | 저가 |
| `ACC_TRDVOL` | string | 거래량 |
| `ACC_TRDVAL` | string | 거래대금 |
| `MKTCAP` | string | 시가총액 |
| `INVSTASST_NETASST_TOTAMT` | string | 순자산총액 |
| `LIST_SHRS` | string | 상장좌수 |
| `IDX_IND_NM` | string | 기초지수_지수명 |
| `OBJ_STKPRC_IDX` | string | 기초지수_종가 |
| `CMPPREVDD_IDX` | string | 기초지수_대비 |
| `FLUC_RT_IDX` | string | 기초지수_등락률 |

## Request Sample

```json
{"basDd":"20230102"}
```

## Response Sample

```json
{"OutBlock_1":[{"BAS_DD":"20230102","ISU_CD":"KR7069500007","ISU_NM":"KODEX 200","TDD_CLSPRC":"27000","CMPPREVDD_PRC":"200","FLUC_RT":"0.75","NAV":"27050","TDD_OPNPRC":"26800","TDD_HGPRC":"27100","TDD_LWPRC":"26750","ACC_TRDVOL":"3000000","ACC_TRDVAL":"81000000000","MKTCAP":"5000000000000","INVSTASST_NETASST_TOTAMT":"5001000000000","LIST_SHRS":"185000000","IDX_IND_NM":"KOSPI200","OBJ_STKPRC_IDX":"350.00","CMPPREVDD_IDX":"2.50","FLUC_RT_IDX":"0.72"}]}
```