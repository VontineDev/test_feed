# 신주인수권증서 일별매매정보

## 개요

- **설명**: 유가증권/코스닥시장에 상장되어 있는 신주인수권증서의 매매정보 제공
- **데이터 제공 시작**: 2010년 02월 12일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/sto/sr_bydd_trd`
- **HTTP Method**: GET

## Request Parameters (InBlock_1)

| Name | Type | Description |
|------|------|-------------|
| `basDd` | string | 기준일자 |

## Response Fields (OutBlock_1)

| Name | Type | Description |
|------|------|-------------|
| `BAS_DD` | string | 기준일자 |
| `MKT_NM` | string | 시장구분 |
| `ISU_CD` | string | 종목코드 |
| `ISU_NM` | string | 종목명 |
| `TDD_CLSPRC` | string | 종가 |
| `CMPPREVDD_PRC` | string | 대비 |
| `FLUC_RT` | string | 등락률 |
| `TDD_OPNPRC` | string | 시가 |
| `TDD_HGPRC` | string | 고가 |
| `TDD_LWPRC` | string | 저가 |
| `ACC_TRDVOL` | string | 거래량 |
| `ACC_TRDVAL` | string | 거래대금 |
| `MKTCAP` | string | 시가총액 |
| `LIST_SHRS` | string | 상장증서수 |
| `ISU_PRC` | string | 신주발행가 |
| `DELIST_DD` | string | 상장폐지일 |
| `TARSTK_ISU_SRT_CD` | string | 목적주권_종목코드 |
| `TARSTK_ISU_NM` | string | 목적주권_종목명 |
| `TARSTK_ISU_PRSNT_PRC` | string | 목적주권_종가 |

## Request Sample

```json
{"basDd":"20230102"}
```

## Response Sample

```json
{"OutBlock_1":[{"BAS_DD":"20230102","MKT_NM":"KOSPI","ISU_CD":"KR7005935R01","ISU_NM":"삼성전자R","TDD_CLSPRC":"200","CMPPREVDD_PRC":"10","FLUC_RT":"5.26","TDD_OPNPRC":"190","TDD_HGPRC":"210","TDD_LWPRC":"190","ACC_TRDVOL":"50000","ACC_TRDVAL":"10000000","MKTCAP":"100000000","LIST_SHRS":"500000","ISU_PRC":"59000","DELIST_DD":"20230131","TARSTK_ISU_SRT_CD":"005930","TARSTK_ISU_NM":"삼성전자","TARSTK_ISU_PRSNT_PRC":"60000"}]}
```