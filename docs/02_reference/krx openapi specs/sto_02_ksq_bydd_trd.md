# 코스닥 일별매매정보

## 개요

- **설명**: 코스닥시장에 상장되어 있는 주권의 매매정보 제공
- **데이터 제공 시작**: 2010년 01월 04일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/sto/ksq_bydd_trd`
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
| `MKT_NM` | string | 시장구분 |
| `SECT_TP_NM` | string | 소속부 |
| `TDD_CLSPRC` | string | 종가 |
| `CMPPREVDD_PRC` | string | 대비 |
| `FLUC_RT` | string | 등락률 |
| `TDD_OPNPRC` | string | 시가 |
| `TDD_HGPRC` | string | 고가 |
| `TDD_LWPRC` | string | 저가 |
| `ACC_TRDVOL` | string | 거래량 |
| `ACC_TRDVAL` | string | 거래대금 |
| `MKTCAP` | string | 시가총액 |
| `LIST_SHRS` | string | 상장주식수 |

## Request Sample

```json
{"basDd":"20230102"}
```

## Response Sample

```json
{"OutBlock_1":[{"BAS_DD":"20230102","ISU_CD":"KR7247540005","ISU_NM":"에코프로비엠","MKT_NM":"KOSDAQ","SECT_TP_NM":"일반기업부","TDD_CLSPRC":"200000","CMPPREVDD_PRC":"5000","FLUC_RT":"2.56","TDD_OPNPRC":"196000","TDD_HGPRC":"202000","TDD_LWPRC":"195000","ACC_TRDVOL":"500000","ACC_TRDVAL":"100000000000","MKTCAP":"15000000000000","LIST_SHRS":"75000000"}]}
```