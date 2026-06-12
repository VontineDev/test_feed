# 일반채권시장 일별매매정보

## 개요

- **설명**: 일반채권시장에 상장되어있는 채권의 매매정보 제공
- **데이터 제공 시작**: 2010년 01월 04일
- **Endpoint**: `https://data-dbg.krx.co.kr/svc/apis/bon/bnd_bydd_trd`
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
| `CLSPRC` | string | 종가_가격 |
| `CMPPREVDD_PRC` | string | 종가_대비 |
| `CLSPRC_YD` | string | 종가_수익률 |
| `OPNPRC` | string | 시가_가격 |
| `OPNPRC_YD` | string | 시가_수익률 |
| `HGPRC` | string | 고가_가격 |
| `HGPRC_YD` | string | 고가_수익률 |
| `LWPRC` | string | 저가_가격 |
| `LWPRC_YD` | string | 저가_수익률 |
| `ACC_TRDVOL` | string | 거래량 |
| `ACC_TRDVAL` | string | 거래대금 |

## Request Sample

```json
{"basDd":"20230102"}
```

## Response Sample

```json
{"OutBlock_1":[{"BAS_DD":"20230102","MKT_NM":"일반채권시장","ISU_CD":"KR2010131D28","ISU_NM":"회사채샘플","CLSPRC":"10000.00","CMPPREVDD_PRC":"0.00","CLSPRC_YD":"4.50","OPNPRC":"10000.00","OPNPRC_YD":"4.50","HGPRC":"10005.00","HGPRC_YD":"4.48","LWPRC":"9995.00","LWPRC_YD":"4.52","ACC_TRDVOL":"100000000","ACC_TRDVAL":"100000000000"}]}
```