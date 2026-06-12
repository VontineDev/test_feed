# ELW 일별매매정보

## 개요

- **설명**: ELW(주식워런트증권)의 매매정보 제공
- **데이터 제공 시작**: 2010년 01월 04일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/etp/elw_bydd_trd`
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
| `TDD_OPNPRC` | string | 시가 |
| `TDD_HGPRC` | string | 고가 |
| `TDD_LWPRC` | string | 저가 |
| `ACC_TRDVOL` | string | 거래량 |
| `ACC_TRDVAL` | string | 거래대금 |
| `MKTCAP` | string | 시가총액 |
| `LIST_SHRS` | string | 상장증권수 |
| `ULY_NM` | string | 기초자산_자산명 |
| `ULY_PRC` | string | 기초자산_종가 |
| `CMPPREVDD_PRC_ULY` | string | 기초자산_대비 |
| `FLUC_RT_ULY` | string | 기초자산_등락률 |

## Request Sample

```json
{"basDd":"20230102"}
```

## Response Sample

```json
{"OutBlock_1":[{"BAS_DD":"20230102","ISU_CD":"KR6005930C35","ISU_NM":"삼성전자콜","TDD_CLSPRC":"50","CMPPREVDD_PRC":"5","TDD_OPNPRC":"45","TDD_HGPRC":"55","TDD_LWPRC":"44","ACC_TRDVOL":"500000","ACC_TRDVAL":"25000000","MKTCAP":"250000000","LIST_SHRS":"5000000","ULY_NM":"삼성전자","ULY_PRC":"60000","CMPPREVDD_PRC_ULY":"500","FLUC_RT_ULY":"0.84"}]}
```