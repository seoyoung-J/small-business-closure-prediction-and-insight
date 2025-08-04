# Databricks notebook source
# MAGIC %pip install gensim
# 사업장명 임베딩 벡터 생성 

import re
import os 
from gensim.models import Word2Vec

# 한국어+영어 섞인 경우만 언더바 처리하는 함수
def fix_korean_english_mixture(sentence):
    if sentence is None:
        return None

    has_korean = bool(re.search(r"[가-힣]", sentence))
    has_english = bool(re.search(r"[a-zA-Z]", sentence))

    if has_korean and has_english:
        
        sentence = re.sub(r"([가-힣])\s+([a-zA-Z])", r"\1_\2", sentence)
        sentence = re.sub(r"([a-zA-Z])\s+([가-힣])", r"\1_\2", sentence)

    return sentence

# 데이터 준비
bizname_df = spark.table("silver.mois.final_business_name")
sentences = bizname_df.select("정제된사업장명").toPandas()["정제된사업장명"].dropna().tolist()

# 한국어-영어 언더바 처리 + 문장 전체를 하나의 토큰으로 구성
tokenized_sentences = [[fix_korean_english_mixture(sentence)] for sentence in sentences if isinstance(sentence, str)]

# Word2Vec 학습
model = Word2Vec(
    sentences=tokenized_sentences,
    vector_size=50,    
    window=5,         
    min_count=1,      
    sg=1,          # Skip-gram 방식 
    workers=8,         
    epochs=30          
)
print("Word2Vec model training completed.")

# 모델 저장
model_dir = "/tmp/word2vec_model"
os.makedirs(model_dir, exist_ok=True)  
model_save_path = os.path.join(model_dir, "word2vec_epoch30.model_re")
model.save(model_save_path)

print(f"Word2Vec 모델 저장 완료: {model_save_path}")

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType, FloatType
import os

model_path = "/tmp/word2vec_model/word2vec_epoch30.model_re"

if not os.path.exists(model_path):
    raise FileNotFoundError(f"Model not found at {model_path}. Train the model first.")

model = Word2Vec.load(model_path)

# 테이블 설정
source_table = "silver.mois.final_business_name"
target_table = "silver.mois.word2vec_embeddings_re"  

# schema 설정
schema = StructType([
    StructField("unique_id", LongType(), True),
    StructField("business_name", StringType(), True),
    StructField("cleaned_business_name", StringType(), True),
    StructField("embedding_array", ArrayType(FloatType()), True)
])

# unique 사업장명 리스트 준비
bizname_df = spark.table("silver.mois.final_business_name").select("정제된사업장명").distinct()
bizname_list = [row["정제된사업장명"] for row in bizname_df.collect() if row["정제된사업장명"]]

# embedding_dict 생성
embedding_dict = {}
for name in bizname_list:
    embedding_dict[name] = model.wv[name] if name in model.wv else [float(0)] * model.vector_size

print(f"Embedding dict created with {len(embedding_dict)} items.")

# row 하나 처리하는 함수
def process_row(row):
    embedding_vec = embedding_dict.get(row["정제된사업장명"], [float(0)] * model.vector_size)
    return Row(
        unique_id=row["unique_id"],
        business_name=row["사업장명"],
        cleaned_business_name=row["정제된사업장명"],
        embedding_array=embedding_vec
    )

# 하나의 batch 처리하는 함수
def process_single_batch(start, batch_size, sub_batch_size, source_table, target_table, total_rows):
    batch_df = (
        spark.table(source_table)
        .orderBy("unique_id")
        .limit(start + batch_size)
        .subtract(
            spark.table(source_table).orderBy("unique_id").limit(start)
        )
    )

    batch_rows = batch_df.select("unique_id", "사업장명", "정제된사업장명").collect()
    print(f"Loaded batch: {len(batch_rows)} rows")

    max_workers = 8
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        result_rows = list(executor.map(process_row, batch_rows))

    print(f"Embedding completed. Saving to Delta table...")

    for i in range(0, len(result_rows), sub_batch_size):
        sub_batch = result_rows[i:i+sub_batch_size]
        sub_batch_sdf = spark.createDataFrame(sub_batch, schema=schema)
        sub_batch_sdf.write.format("delta").mode("append").saveAsTable(target_table)
        print(f"Saved: {start + i + len(sub_batch)} / {total_rows} rows ({(start + i + len(sub_batch))/total_rows*100:.2f}%)")

    print(f"Batch {start} ~ {start + batch_size - 1} completed.")
    return 

# 전체 batch 처리하는 함수
def process_all_batches(source_table, target_table, batch_size=10000, sub_batch_size=5000):
    total_rows = spark.table(source_table).count()
    print(f"Total number of rows: {total_rows}")

    # 새 테이블 덮어쓰기
    spark.sql(f"DROP TABLE IF EXISTS {target_table}")

    for start in range(0, total_rows, batch_size):
        print(f"\nProcessing batch: {start} ~ {min(start + batch_size - 1, total_rows - 1)}")
        process_single_batch(start, batch_size, sub_batch_size, source_table, target_table, total_rows)

    print("\n All batches completed. Embedding finished!")
    return

# 실행
process_all_batches(source_table, target_table, batch_size=10000, sub_batch_size=5000)