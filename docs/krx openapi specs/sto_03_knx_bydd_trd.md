# 코넥스 일별매매정보

## 개요

- **설명**: 코넥스시장에 상장되어 있는 주권의 매매정보 제공
- **데이터 제공 시작**: 2013년 07월 01일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/sto/knx_bydd_trd`
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
{"OutBlock_1":[{"BAS_DD":"20230102","ISU_CD":"KR7000010000","ISU_NM":"코넥스샘플","MKT_NM":"KONEX","SECT_TP_NM":"-","TDD_CLSPRC":"5000","CMPPREVDD_PRC":"100","FLUC_RT":"2.04","TDD_OPNPRC":"4900","TDD_HGPRC":"5100","TDD_LWPRC":"4900","ACC_TRDVOL":"10000","ACC_TRDVAL":"50000000","MKTCAP":"50000000000","LIST_SHRS":"10000000"}]}
```