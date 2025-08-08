# 폐업 예측 모델 실험 (RandomForest) 
# 계층 샘플링 기반 Train/Test 분할
# 클래스 불균형 처리 (클래스 가중치 / 언더샘플링 / 오버샘플링)
# 오버샘플링 방식 학습 및 하이퍼파라미터 튜닝 포함
# 최종 모델은 XGBoost이나, 본 코드는 실험 비교용 RandomForest baseline 결과 기록용

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score, confusion_matrix, roc_curve
from sklearn.utils import resample
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.font_manager as fm

# 시각화 관련 설정 
font_path = "/Volumes/asacdataanalysis/temp/fonts/NanumGothic.ttf"
fm.fontManager.addfont(font_path)
plt.rcParams['font.family'] = fm.FontProperties(fname=font_path).get_name()

# 1. 평가 함수 정의
def evaluate_model(model_name, y_true, y_pred, y_proba):
    print(f"[{model_name}] 결과")
    print(classification_report(y_true, y_pred, digits=4))
    print(f"AUC: {roc_auc_score(y_true, y_proba):.4f}\n")

def plot_model_results(y_true, y_pred, y_proba, feature_names, importances, title):
    cm = confusion_matrix(y_true, y_pred)
    fpr, tpr, _ = roc_curve(y_true, y_proba)
    auc_score = roc_auc_score(y_true, y_proba)
    
    importance_df = pd.DataFrame({
        "Feature": feature_names,
        "Importance": importances
    }).sort_values(by="Importance", ascending=False)

    fig, axes = plt.subplots(1, 3, figsize=(20, 6))
    fig.suptitle(title, fontsize=16, weight="bold")

    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
                xticklabels=['운영중(0)', '폐업(1)'],
                yticklabels=['운영중(0)', '폐업(1)'],
                ax=axes[0])
    axes[0].set_title("혼동 행렬")
    axes[0].set_xlabel("예측값")
    axes[0].set_ylabel("실제값")

    axes[1].plot(fpr, tpr, label=f"AUC = {auc_score:.4f}")
    axes[1].plot([0, 1], [0, 1], 'k--')
    axes[1].set_title("ROC 커브")
    axes[1].set_xlabel("False Positive Rate")
    axes[1].set_ylabel("True Positive Rate")
    axes[1].legend()

    sns.barplot(x="Importance", y="Feature", data=importance_df, ax=axes[2], palette="viridis")
    axes[2].set_title("변수 중요도")
    axes[2].set_xlabel("중요도")
    axes[2].set_ylabel("변수명")

    plt.tight_layout()
    plt.show()


# 2. 데이터 불러오기 및 계층 샘플링
df_spark = spark.table("silver.mois.mois_model_method2")
selected_cols = [
    "폐업유무", 
    "시도_index", 
    "시군구_index", 
    "법정동_index", 
    "운영기간_index", 
    "개방서비스명_index", 
    "업태구분명_통합_인덱스"
]
df = df_spark.select(*selected_cols).toPandas()

# stratify key 생성 및 Train/Test 분할
df["stratify_key"] = df["개방서비스명_index"].astype(str) + "_" + df["폐업유무"].astype(str)
train_df, test_df = train_test_split(df, test_size=0.1, stratify=df["stratify_key"], random_state=42)
train_df = train_df.drop(columns=["stratify_key"])
test_df = test_df.drop(columns=["stratify_key"])


# 3. Train 데이터에서 100만 개 계층 샘플링
sample_df = train_df.groupby('폐업유무', group_keys=False).apply(
    lambda x: x.sample(frac=1000000 / len(train_df), random_state=42)
).reset_index(drop=True)


# 4. X, y 분리
feature_cols = [
    "시도_index", "시군구_index", "법정동_index",
    "운영기간_index", "개방서비스명_index", "업태구분명_통합_인덱스"
]

X_train_sampled = sample_df[feature_cols]
y_train_sampled = sample_df["폐업유무"]
X_test = test_df[feature_cols]
y_test = test_df["폐업유무"]


# 5. 클래스 불균형 처리 실험

# 5-1. 클래스 가중치
rf = RandomForestClassifier(n_estimators=100, max_depth=10, class_weight='balanced', random_state=42, n_jobs=-1)
rf.fit(X_train_sampled, y_train_sampled)
evaluate_model("RandomForest (클래스 가중치)", y_test, rf.predict(X_test), rf.predict_proba(X_test)[:, 1])

# 5-2. 언더샘플링
df_major = sample_df[sample_df["폐업유무"] == 0]
df_minor = sample_df[sample_df["폐업유무"] == 1]
df_major_under = resample(df_major, replace=False, n_samples=len(df_minor), random_state=42)
df_under = pd.concat([df_major_under, df_minor])
X_train_under = df_under[feature_cols]
y_train_under = df_under["폐업유무"]

rf_under = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42, n_jobs=-1)
rf_under.fit(X_train_under, y_train_under)
evaluate_model("RandomForest (언더샘플링)", y_test, rf_under.predict(X_test), rf_under.predict_proba(X_test)[:, 1])

# 5-3. 오버샘플링
df_minor_over = resample(df_minor, replace=True, n_samples=len(df_major), random_state=42)
df_over = pd.concat([df_major, df_minor_over]).sample(frac=1, random_state=42)
X_train_over = df_over[feature_cols]
y_train_over = df_over["폐업유무"]

rf_over = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42, n_jobs=-1)
rf_over.fit(X_train_over, y_train_over)
y_pred_over = rf_over.predict(X_test)
y_proba_over = rf_over.predict_proba(X_test)[:, 1]
evaluate_model("RandomForest (오버샘플링)", y_test, y_pred_over, y_proba_over)
plot_model_results(y_test, y_pred_over, y_proba_over, X_train_over.columns, rf_over.feature_importances_, "RandomForest (오버샘플링)")


# 6. 하이퍼파라미터 튜닝 (오버샘플링 기반)
param_grid = {
    "max_depth": [10, 20],
    "min_samples_split": [2, 5],
    "min_samples_leaf": [1, 2]
}

grid = GridSearchCV(
    estimator=RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1),
    param_grid=param_grid,
    scoring="f1",
    cv=3,
    n_jobs=-1,
    verbose=2
)
grid.fit(X_train_over, y_train_over)
best_model = grid.best_estimator_

y_pred_best = best_model.predict(X_test)
y_proba_best = best_model.predict_proba(X_test)[:, 1]
evaluate_model("RandomForest (오버샘플링 + 튜닝)", y_test, y_pred_best, y_proba_best)
plot_model_results(y_test, y_pred_best, y_proba_best, X_train_over.columns, best_model.feature_importances_, "RandomForest (오버샘플링 + 튜닝)")
