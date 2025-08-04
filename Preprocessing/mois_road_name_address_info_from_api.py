# Databricks notebook source
# 도로명주소로부터 시도, 시군구, 법정동 정보를 수집하는 전처리 코드ㅡ 
# 행정안전부 도로명주소 API를 병렬 호출하여 Delta 테이블로 저장

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession, Row

import requests
import json
import ast

import threading
import concurrent.futures 
import re 

access_key = 'API_KEY' 

def clean_address(addr):
    if addr is None:
        return None
    addr = re.sub(r'\(([^()]*)\)', lambda m: '(' + m.group(1).split(',')[0] + ')', addr) # 괄호 안 쉼표 이후 제거 
    addr = re.sub(r'\s+', ' ', addr).strip()
    return addr

lock = threading.Lock()

# Function to parse a single juso element
def convert_dict_to_df(response_dict, schema):
    common = response_dict['results']['common']
    juso_list = response_dict['results']['juso']
    
    rows = []
    
    for juso in juso_list:
        row_data = {
            'totalCount': common.get('totalCount'),
            'currentPage': common.get('currentPage'),
            'countPerPage': common.get('countPerPage'),
            'errorCode': common.get('errorCode'),
            'errorMessage': common.get('errorMessage'),
        }
        for field in schema.fieldNames():
            if field not in row_data:
                row_data[field] = juso.get(field)
        rows.append(Row(**row_data))
    
    return rows
    

def get_address_data(access_key, address, schema):
    params = {
        'confmKey': access_key,
        'currentPage': '1',
        'countPerPage': '1',
        'addInfoYn': 'Y',
        'keyword': address,
        'resultType': 'json'
    }

    url = 'https://business.juso.go.kr/addrlink/addrLinkApiJsonp.do'

    try:
        response = requests.get(url, params=params)

        if response.status_code != 200:
            print(f"HTTP Error ({response.status_code}) for address: {address}")
            return None

        # JSONP → JSON 문자열 파싱
        response_text = response.text.strip()
        if response_text.startswith("(") and response_text.endswith(")"):
            clean_text = response_text[1:-1]
        else:
            clean_text = response_text

        res_dict = ast.literal_eval(clean_text)

        if (
            'results' not in res_dict or
            'juso' not in res_dict['results'] or
            not res_dict['results']['juso'] or
            res_dict['results']['common'].get('totalCount') == '0'
        ):
            print(f"Warning: No result data for address: {address}")
            return None

        return res_dict

    except Exception as e:
        print(f"Error fetching data for address: {address}. Error: {e}")
        return None

def process_address_data(access_key, unique_id, address_counter, address_input, schema):
    address_data = get_address_data(access_key, address_input, schema)
    
    if address_data is None:
        result_data = {
            'totalCount': None,
            'currentPage': None,
            'countPerPage': None,
            'errorCode': None,
            'errorMessage': None, 
            'uid': str(unique_id),
            'id': str(address_counter),
            'full_address': address_input,
            'roadAddr': None,
            'roadAddrPart1': None, 
            'roadAddrPart2': None, 
            'jibunAddr': None,
            'engAddr': None,
            'zipNo': None,
            'admCd': None,
            'rnMgtSn': None,
            'bdMgtSn': None,
            'detBdNmList': None,
            'bdNm': None,
            'bdKdcd': None,
            'siNm': None,
            'sggNm': None,
            'emdNm': None,
            'liNm': None,
            'rn': None,
            'udrtYn': None,
            'buldMnnm': None,
            'buldSlno': None,
            'mtYn': None,
            'lnbrMnnm': None,
            'lnbrSlno': None,
            'emdNo': None,
            'hstryYn': None,
            'relJibun': None,
            'hemdNm': None
        }
        return [Row(**result_data)]

    row_list = convert_dict_to_df(address_data, schema)
    
    enriched_rows = []
    for row in row_list:
        row_dict = row.asDict()
        row_dict['uid'] = str(unique_id)
        row_dict['id'] = str(address_counter)
        row_dict['full_address'] = address_input
        enriched_rows.append(Row(**row_dict))

    return enriched_rows

def process_in_chunks(address_list, access_token, chunk_size=100):
    schema = T.StructType([
        T.StructField("totalCount", T.StringType(), True),
        T.StructField("currentPage", T.StringType(), True),
        T.StructField("countPerPage", T.StringType(), True),
        T.StructField("errorCode", T.StringType(), True),
        T.StructField("errorMessage", T.StringType(), True),
        T.StructField("uid", T.StringType(), True),
        T.StructField("id", T.StringType(), True), 
        T.StructField("full_address", T.StringType(), True),
        T.StructField("roadAddr", T.StringType(), True),
        T.StructField("roadAddrPart1", T.StringType(), True),
        T.StructField("roadAddrPart2", T.StringType(), True),
        T.StructField("jibunAddr", T.StringType(), True),
        T.StructField("engAddr", T.StringType(), True),
        T.StructField("zipNo", T.StringType(), True),
        T.StructField("admCd", T.StringType(), True),
        T.StructField("rnMgtSn", T.StringType(), True),
        T.StructField("bdMgtSn", T.StringType(), True),
        T.StructField("detBdNmList", T.StringType(), True),
        T.StructField("bdNm", T.StringType(), True),
        T.StructField("bdKdcd", T.StringType(), True),
        T.StructField("siNm", T.StringType(), True),
        T.StructField("sggNm", T.StringType(), True),
        T.StructField("emdNm", T.StringType(), True),
        T.StructField("liNm", T.StringType(), True),
        T.StructField("rn", T.StringType(), True),
        T.StructField("udrtYn", T.StringType(), True),
        T.StructField("buldMnnm", T.StringType(), True),
        T.StructField("buldSlno", T.StringType(), True),
        T.StructField("mtYn", T.StringType(), True),
        T.StructField("lnbrMnnm", T.StringType(), True),
        T.StructField("lnbrSlno", T.StringType(), True),
        T.StructField("emdNo", T.StringType(), True),
        T.StructField("hstryYn", T.StringType(), True),
        T.StructField("relJibun", T.StringType(), True),
        T.StructField("hemdNm", T.StringType(), True)
    ])

    result_batch = []
    processed_set = set()

    chunked_address_list = [address_list[i:i + chunk_size] for i in range(0, len(address_list), chunk_size)]

    for chunk in chunked_address_list:
        futures = []

        with concurrent.futures.ThreadPoolExecutor() as executor:
            for row in chunk:
                raw_address = row['full_address']
                if raw_address is None:
                    continue

                address_input = clean_address(raw_address)
                address_counter = row['row_num']     
                unique_id = row['id']               

                futures.append(
                    executor.submit(
                        process_address_data, access_token, unique_id, address_counter, address_input, schema
                    )
                )

            for future in concurrent.futures.as_completed(futures):
                row_list = future.result()  # List[Row]

                if row_list is None:
                    continue

                with lock:
                    for row in row_list:
                        row_dict = row.asDict()
                        unique_key = (row_dict['uid'], row_dict['full_address'])
                        if unique_key not in processed_set:
                            processed_set.add(unique_key)
                            result_batch.append(row_dict)

        if result_batch:
            spark_df = spark.createDataFrame(result_batch, schema)
            spark_df = spark_df.withColumn(
                'created_at',
                F.date_format(F.expr("current_timestamp() + interval 9 hours"), "yyyy-MM-dd HH:mm:ss")
            )

            spark_df.write.format("delta") \
                .mode("append") \
                .saveAsTable("bronze.file_mois.mois_address_raw_v2")

            spark_df.select(
                "uid", "id", "full_address", "siNm", "sggNm", "emdNm"
            ).write.format("delta") \
            .mode("append") \
            .saveAsTable("silver.mois.mois_address_info_v2")

            result_batch.clear()


# 좌표 컬럼 null인것 중에 도로명전체주소가 존재하는 리스트 추출(Row 객체 기반)
address_list = spark.sql("""
SELECT 
  unique_id as id,              
  `번호` as row_num, 
  `도로명전체주소` as full_address
FROM silver.mois.mois_table_with_id
WHERE 
  (`좌표정보x_epsg5174_` IS NULL OR `좌표정보y_epsg5174_` IS NULL)
  AND `도로명전체주소` IS NOT NULL 
  AND TRIM(`도로명전체주소`) != ''
""").collect() 

# 실행 
process_in_chunks(address_list, access_key) 
