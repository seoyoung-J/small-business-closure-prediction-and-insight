# Databricks notebook source
# MAGIC %md
# MAGIC ### 1차 전처리 
# MAGIC - 정제 결과가 없는(isnull()) 경우만 골라서 별도로 처리
# MAGIC - 그중 괄호 안 영어가 있는 경우 복구
# MAGIC - 지점, 지점명, 센터 등 제외 
# MAGIC - 사업장명+지점명 붙어있는 경우 보류 처리 

# COMMAND ----------

import re
import pandas as pd

# 사업장명 1차 전처리 함수 
def clean_bizname_final(name):
    if name is None or not isinstance(name, str):
        return None

    name = re.sub(r"\s+", " ", name).strip()
    name = name.replace('(주)', '').replace('( 주)', '').replace('(주', '').replace('주)', '').replace('( 주', '')
    name = name.replace('주식회사', '')
    name = name.replace('-한시적', '').replace(' -한시적', '').replace('(한시적)', '')

    eng_in_parens = re.findall(r"\(([^)]*[a-z][^)]*)\)", name)

    name = re.sub(r"\([^)]*\)", "", name).strip()
    name = re.sub(r"\s+\S{1,20}(지점|점|센터|분점)$", "", name)

    if re.search(r'[가-힣a-z0-9]{2,}(지점|점|센터|분점)$', name):
        return "보류"

    if re.fullmatch(r"[^\w\s]", name):
        return None

    name = name.strip()
    if name == "":
        return None

    if name == "주" and not eng_in_parens:
        return None

    if eng_in_parens:
        return f"{name} {' '.join(eng_in_parens)}"

    return name

# 괄호 안에 영어 있는 경우 전처리 함수 (괄호만 제거 후 소문자로 변경)
def recover_english_in_parentheses(name):
    if isinstance(name, str):
        match = re.search(r"\(([^)]*[a-zA-Z][^)]*)\)", name)
        if match:
            # 괄호 안에 영어 있는 경우
            clean_name = re.sub(r"[()]", "", name).strip()
            return clean_name.lower()
    return None 

# 1차 전처리 적용 함수 
def process_business_names(df):
    df_bizname_pd = df.select("unique_id", "사업장명").toPandas()

    df_bizname_pd["정제된사업장명"] = df_bizname_pd["사업장명"].apply(clean_bizname_final)
    df_bizname_pd["정제결과"] = df_bizname_pd["정제된사업장명"]

    valid_all_df = df_bizname_pd[df_bizname_pd["정제결과"].isnull()][["unique_id", "사업장명", "정제된사업장명", "정제결과"]].copy()

    valid_all_df["정제된사업장명"] = valid_all_df["사업장명"].apply(recover_english_in_parentheses)
    valid_all_df["정제결과"] = valid_all_df["정제된사업장명"]

    valid_all_df = valid_all_df[
        (valid_all_df["정제된사업장명"].notnull()) & 
        (valid_all_df["정제된사업장명"].str.lower() != "null")
    ].copy()

    cleaned_business_name_df = df_bizname_pd.copy()
    cleaned_business_name_df.set_index("unique_id", inplace=True)
    valid_all_df.set_index("unique_id", inplace=True)
    cleaned_business_name_df.update(valid_all_df[["정제된사업장명", "정제결과"]])
    cleaned_business_name_df.reset_index(inplace=True)

    return cleaned_business_name_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2차 전처리 
# MAGIC - 보류로 남긴 것 2차 전처리 
# MAGIC - 1차 전처리 완료된 것 중에서 사전 생성하여 사업장명 추출 후 적용  

# COMMAND ----------

# 보류로 처리한 데이터 전처리하는데 필요한 함수들 

# 관련 텍스트만 제거하는 함수
def clean_parentheses_ju(name):
    if name is None or not isinstance(name, str):
        return None
    name = re.sub(r"\s*\( ?주\)?\s*", "", name)
    name = name.replace('주식회사', '')
    return name.strip()

# 브랜드 매칭 함수
def extract_from_dict_better(name, brand_list):
    if not isinstance(name, str):
        return None

    for brand in brand_list:
        if len(brand) <= 2:
            if re.search(r'[a-z]', brand):
                if re.search(rf'\b{re.escape(brand)}\b', name):
                    return brand
            else:
                if brand in name:
                    return brand
        else:
            if brand in name:
                return brand
    return None

# 최종 전처리 함수
def process_final_business_names(df, brand_top_n=500, min_brand_freq=500, words_to_remove=None):
    if words_to_remove is None:
        words_to_remove = [
            "중앙", "잡화", "우리", "우정", "제일",
            "그린", "대성", "드림", "타임", "스타",
            "마루", "명가", "정성", "코코", "현대"
        ]

    df_bizname_cleaned = df.copy()

    df_hold = df_bizname_cleaned[df_bizname_cleaned["정제결과"] == "보류"].copy()
    df_vocab_source = df_bizname_cleaned[df_bizname_cleaned["정제결과"] != "보류"].copy()

    # 단어사전 만들기
    brand_dict = (
        df_vocab_source[df_vocab_source["정제결과"].str.len() >= 2]["정제결과"]
        .value_counts()
        .head(brand_top_n)
        .index
        .tolist()
    )
    brand_dict_sorted = sorted(brand_dict, key=lambda x: -len(x))  # 긴 것부터 매칭

    # 보류 데이터 전처리
    df_hold["사업장명_cleaned"] = df_hold["사업장명"].apply(clean_parentheses_ju)

    # 대표상호 추출
    df_hold["대표상호"] = df_hold["사업장명_cleaned"].apply(lambda x: extract_from_dict_better(x, brand_dict_sorted))

    # 등장횟수 기준 필터
    brand_counts = df_hold["대표상호"].value_counts()
    valid_brands_final = brand_counts[brand_counts >= min_brand_freq].index.tolist()
    df_hold_filtered = df_hold[df_hold["대표상호"].isin(valid_brands_final)].copy()

    # 의미 없는 단어 제거
    df_hold_filtered.loc[
        df_hold_filtered["대표상호"].isin(words_to_remove),
        "대표상호"
    ] = None

    # None 제거
    df_hold_valid = df_hold_filtered[df_hold_filtered["대표상호"].notnull()].copy()

    # 정제된사업장명, 정제결과 갱신
    df_hold_valid["정제된사업장명"] = df_hold_valid["대표상호"]
    df_hold_valid["정제결과"] = df_hold_valid["대표상호"]

    # 필요한 컬럼만 유지
    df_hold_valid = df_hold_valid[["unique_id", "사업장명", "정제된사업장명", "정제결과"]].copy()

    # 보류아닌 것과 합치기
    final_business_name_df = pd.concat([df_vocab_source, df_hold_valid], ignore_index=True)

    return final_business_name_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### 임베딩을 위한 전처리  

# COMMAND ----------

import re

# 숫자만 있는 경우 처리 함수
def is_only_number_except_year(name):
    if not isinstance(name, str):
        return False
    if name.isdigit():
        return len(name) == 4
    return True

# 특수문자 정리 함수
def clean_special_tokens(name):
    if not isinstance(name, str):
        return name
    name = re.sub(r"[()]", "", name)
    name = name.replace(",", " ").replace("/", " ")
    name = re.sub(r"[^가-힣a-z0-9\s\-&\.·]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name

# 임베딩을 위한 전처리 함수 
def preprocess_for_embedding(df):
    # 브랜드 통일 매핑
    brand_standard_mapping = {
        "씨유": "CU",
        "씨유 CU": "CU",
        "CU": "CU",
        "지에스25": "GS25",
        "지에스25 GS": "GS25",
        "지에스25 GS25": "GS25",
        "GS25": "GS25",
        "이디야": "이디야커피",
        "이디야커피": "이디야커피",
    }
    df["정제된사업장명"] = df["정제된사업장명"].replace(brand_standard_mapping)
    df["정제결과"] = df["정제결과"].replace(brand_standard_mapping)

    # 소문자 변환
    df["정제된사업장명"] = df["정제된사업장명"].str.lower()
    df["정제결과"] = df["정제결과"].str.lower()

    # Null 제거
    df = df[df["정제된사업장명"].notnull()].copy()

    # 숫자만 존재하는 경우 처리
    df = df[df["정제된사업장명"].apply(is_only_number_except_year)].copy()

    # 특수문자 정리
    df["정제된사업장명"] = df["정제된사업장명"].apply(clean_special_tokens)
    df["정제결과"] = df["정제결과"].apply(clean_special_tokens)

    # 1글자 이하 제거
    df = df[df["정제된사업장명"].apply(lambda x: len(x.strip()) > 1)].copy()

    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ### 최종 전처리 완료된 df 생성  

# COMMAND ----------

# 1차 기본 전처리
df = spark.table("silver.mois.mois_deltatable_new") 
cleaned_business_name_df = process_business_names(df) # 1차 기본 전처리 
final_business_name_df = process_final_business_names(cleaned_business_name_df) # 2차 전처리 
final_business_name_df = preprocess_for_embedding(final_business_name_df) # 임베딩용 최종 전처리 적용

print("사업장명 전처리 후 행 수:", len(cleaned_business_name_df)) 
print("최종 전처리 완료된 데이터프레임 수:", len(final_business_name_df)) 
print("임베딩용 전처리 완료 후 데이터 수:", len(final_business_name_df))
display(final_business_name_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### delta table 저장 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
    StructField("unique_id", LongType(), True),        # int64 -> LongType
    StructField("사업장명", StringType(), True),        # object -> StringType
    StructField("정제된사업장명", StringType(), True),
    StructField("정제결과", StringType(), True),
])

# pandas DataFrame -> Spark DataFrame
final_business_name = spark.createDataFrame(final_business_name_df, schema=schema)

# Delta 테이블로 저장
final_business_name.write.format("delta").mode("overwrite").saveAsTable("silver.mois.final_business_name")