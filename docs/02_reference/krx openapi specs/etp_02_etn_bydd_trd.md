# ETN 일별매매정보

## 개요

- **설명**: ETN(상장지수증권)의 매매정보 제공
- **데이터 제공 시작**: 2014년 11월 17일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/etp/etn_bydd_trd`
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
| `PER1SECU_INDIC_VAL` | string | 지표가치(IV) |
| `TDD_OPNPRC` | string | 시가 |
| `TDD_HGPRC` | string | 고가 |
| `TDD_LWPRC` | string | 저가 |
| `ACC_TRDVOL` | string | 거래량 |
| `ACC_TRDVAL` | string | 거래대금 |
| `MKTCAP` | string | 시가총액 |
| `INDIC_VAL_AMT` | string | 지표가치총액 |
| `LIST_SHRS` | string | 상장증권수 |
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
{"OutBlock_1":[{"BAS_DD":"20230102","ISU_CD":"KR7500030004","ISU_NM":"삼성 레버리지 KOSPI200 ETN","TDD_CLSPRC":"15000","CMPPREVDD_PRC":"300","FLUC_RT":"2.04","PER1SECU_INDIC_VAL":"15020","TDD_OPNPRC":"14700","TDD_HGPRC":"15100","TDD_LWPRC":"14650","ACC_TRDVOL":"100000","ACC_TRDVAL":"1500000000","MKTCAP":"750000000000","INDIC_VAL_AMT":"751000000000","LIST_SHRS":"50000000","IDX_IND_NM":"레버리지 KOSPI200","OBJ_STKPRC_IDX":"700.00","CMPPREVDD_IDX":"14.00","FLUC_RT_IDX":"2.04"}]}
```