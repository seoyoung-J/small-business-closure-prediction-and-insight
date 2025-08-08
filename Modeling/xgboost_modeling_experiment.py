# Databricks notebook source
# XGBoost 모델링 (임베딩 vs 프랜차이즈 여부)

# COMMAND ----------
# 라이브러리 설치
# %pip install xgboost optuna scikit-optimize imbalanced-learn 

# COMMAND ----------
# 폰트 설정
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.font_manager as fm

font_path = "/Volumes/asacdataanalysis/temp/fonts/NanumGothic.ttf"
fm.fontManager.addfont(font_path)
plt.rcParams['font.family'] = fm.FontProperties(fname=font_path).get_name()

# COMMAND ----------
# 데이터 로드 및 필터링
from pyspark.sql.functions import col, year, when, concat_ws

base_df = spark.table("silver.mois.mois_model")
filtered_df = base_df.filter((year(col("폐업일자_날짜")) >= 2020) | (col("폐업일자_날짜").isNull()))

include_franchise = True  # False 시 기본 임베딩 벡터 조인 방식 전처리 

if include_franchise:
    bizname_df = spark.table("silver.mois.final_business_name").select("unique_id", "정제된사업장명")
    name_counts = bizname_df.groupBy("정제된사업장명").count()
    name_counts_pd = name_counts.toPandas()
    franchise_names = name_counts_pd[name_counts_pd["count"] >= 20]["정제된사업장명"].tolist()
    bizname_df = bizname_df.withColumn("is_franchise", when(col("정제된사업장명").isin(franchise_names), 1).otherwise(0))
    franchise_info = bizname_df.select("unique_id", "is_franchise")
    if "is_franchise" in filtered_df.columns:
        filtered_df = filtered_df.drop("is_franchise")
    filtered_df = filtered_df.join(franchise_info, on="unique_id", how="left").fillna({"is_franchise": 0})
else:
    embedding_df_raw = spark.table("silver.mois.word2vec_embeddings_flattened")
    embedding_cols = [c for c in embedding_df_raw.columns if c.startswith("embedding_")]
    embedding_df = embedding_df_raw.select(["unique_id"] + embedding_cols)
    filtered_df = filtered_df.join(embedding_df, on="unique_id", how="left")
    from pyspark.sql.functions import coalesce, lit
    for col_name in embedding_cols:
        filtered_df = filtered_df.withColumn(col_name, coalesce(col(col_name), lit(0.0)))

# COMMAND ----------
# 계층적 분할 및 학습용 샘플 추출
df = filtered_df.withColumn("stratify_col", concat_ws("_", col("개방서비스명").cast("string"), col("폐업유무").cast("string")))
strata_list = [row["stratify_col"] for row in df.select("stratify_col").distinct().collect()]
fractions = {key: 0.1 for key in strata_list}
test_df = df.stat.sampleBy("stratify_col", fractions=fractions, seed=42)
train_df = df.join(test_df, on="unique_id", how="left_anti").drop("stratify_col")
test_df = test_df.drop("stratify_col")

# COMMAND ----------
# 70만 계층 샘플링
from pyspark.sql.functions import col

target = 700_000
frac = target / train_df.count()
sampled = train_df.stat.sampleBy("폐업유무", fractions={0: frac, 1: frac}, seed=42)
sample_df = sampled.limit(target).toPandas()

# COMMAND ----------
# X, y 구성
X = sample_df.drop(columns=["unique_id", "개방서비스명", "시도", "시군구", "법정동", "운영기간", "운영기간_범주", "업태구분명_통합", "폐업유무", "폐업일자_날짜", "법정동_index"])
y = sample_df['폐업유무']

# COMMAND ----------
# train/test 분할
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)

# COMMAND ----------
# 오버샘플링 및 XGBoost 학습
from imblearn.over_sampling import RandomOverSampler
from xgboost import XGBClassifier
from sklearn.metrics import classification_report

ros = RandomOverSampler(random_state=42)
X_train_resampled, y_train_resampled = ros.fit_resample(X_train, y_train)

model = XGBClassifier(objective="binary:logistic", use_label_encoder=False, eval_metric='logloss', n_estimators=100, max_depth=6, learning_rate=0.1)
model.fit(X_train_resampled, y_train_resampled)

# 평가 출력
y_pred = model.predict(X_test)
print(classification_report(y_test, y_pred, digits=4))

# COMMAND ----------
# Feature Importance 시각화
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

feature_importances = pd.Series(model.feature_importances_, index=X.columns)
top_features = feature_importances.sort_values(ascending=False).head(20)
plt.figure(figsize=(10, 8))
sns.barplot(x=top_features.values, y=top_features.index)
plt.title('XGBoost Feature Importance (Top 20)', fontsize=16)
plt.xlabel('중요도', fontsize=14)
plt.ylabel('피처 이름', fontsize=14)
plt.grid(axis='x')
plt.show()

# COMMAND ----------
# ROC 곡선 시각화
from sklearn.metrics import roc_curve, auc

y_proba = model.predict_proba(X_test)[:, 1]
fpr, tpr, _ = roc_curve(y_test, y_proba)
roc_auc = auc(fpr, tpr)

plt.figure(figsize=(6, 4))
plt.plot(fpr, tpr, label=f'AUC = {roc_auc:.4f}')
plt.plot([0, 1], [0, 1], 'k--')
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('ROC Curve')
plt.legend()
plt.grid(True)
plt.show()

# COMMAND ----------
# PR 곡선 시각화
from sklearn.metrics import precision_recall_curve

precision, recall, _ = precision_recall_curve(y_test, y_proba)
plt.figure(figsize=(6, 4))
plt.plot(recall, precision, label='정밀도-재현율 곡선')
plt.xlabel('재현율 (Recall)')
plt.ylabel('정밀도 (Precision)')
plt.title('Precision-Recall 곡선')
plt.grid(True)
plt.legend()
plt.show()

# COMMAND ----------
# 예측 확률 분포 시각화
plt.figure(figsize=(7, 5))
sns.histplot(y_proba[y_test == 1], color='red', label='폐업 (1)', kde=True)
sns.histplot(y_proba[y_test == 0], color='blue', label='운영중 (0)', kde=True)
plt.xlabel('폐업 확률 예측값')
plt.title('예측 확률 분포 (운영중 vs 폐업)')
plt.legend()
plt.grid(True)
plt.show()
