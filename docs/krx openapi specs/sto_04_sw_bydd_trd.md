# 신주인수권증권 일별매매정보

## 개요

- **설명**: 유가증권/코스닥시장에 상장되어 있는 신주인수권증권의 매매정보 제공
- **데이터 제공 시작**: 2010년 01월 04일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/sto/sw_bydd_trd`
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
| `LIST_SHRS` | string | 상장증권수 |
| `EXER_PRC` | string | 행사가격 |
| `EXST_STRT_DD` | string | 존속기간_시작일 |
| `EXST_END_DD` | string | 존속기간_종료일 |
| `TARSTK_ISU_SRT_CD` | string | 목적주권_종목코드 |
| `TARSTK_ISU_NM` | string | 목적주권_종목명 |
| `TARSTK_ISU_PRSNT_PRC` | string | 목적주권_종가 |

## Request Sample

```json
{"basDd":"20230102"}
```

## Response Sample

```json
{"OutBlock_1":[{"BAS_DD":"20230102","MKT_NM":"KOSPI","ISU_CD":"KR7005935W01","ISU_NM":"삼성전자W","TDD_CLSPRC":"100","CMPPREVDD_PRC":"5","FLUC_RT":"5.26","TDD_OPNPRC":"95","TDD_HGPRC":"105","TDD_LWPRC":"95","ACC_TRDVOL":"100000","ACC_TRDVAL":"10000000","MKTCAP":"500000000","LIST_SHRS":"5000000","EXER_PRC":"55000","EXST_STRT_DD":"20220101","EXST_END_DD":"20250101","TARSTK_ISU_SRT_CD":"005930","TARSTK_ISU_NM":"삼성전자","TARSTK_ISU_PRSNT_PRC":"60000"}]}
```