"""
Shell Gas Station - Store Conversion ML Model (MLOps)
------------------------------------------------------
Binary classifier: predicts which fuel customers will walk into the store.

Model   : XGBoost (tree-based, handles mixed types natively)
MLOps   : MLflow experiment tracking + Databricks Model Registry
Explain : SHAP feature importance (global + per-prediction)
Split   : Temporal — train Jan-Sep 2025, test Oct-Dec 2025 (no leakage)
"""

# %pip install xgboost shap --quiet   # uncomment when running in a Databricks notebook

import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
import shap
import xgboost as xgb
from mlflow.models.signature import infer_signature
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from sklearn.metrics import (
    accuracy_score, classification_report, confusion_matrix,
    f1_score, precision_score, recall_score, roc_auc_score,
)
from sklearn.preprocessing import OrdinalEncoder

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
CATALOG          = "main"
SCHEMA           = "shell_gas"
EXPERIMENT_NAME  = "/Shared/shell_store_conversion"
MODEL_NAME       = "shell_store_conversion_xgb"
TEST_CUTOFF      = "2025-10-01"   # temporal train/test boundary

XGB_PARAMS = {
    "n_estimators":      400,
    "max_depth":         5,
    "learning_rate":     0.05,
    "subsample":         0.8,
    "colsample_bytree":  0.8,
    "min_child_weight":  5,
    "scale_pos_weight":  1.6,   # ~62/38 negative/positive ratio
    "eval_metric":       "auc",
    "use_label_encoder": False,
    "random_state":      42,
}

CAT_FEATURES = ["fuel_type", "payment_method", "loyalty_tier", "preferred_fuel"]

spark = SparkSession.builder.appName("ShellStoreConversionML").getOrCreate()


# --------------------------------------------------------------------------- #
# 1. Load raw tables
# --------------------------------------------------------------------------- #
def load_tables():
    fuel  = spark.table(f"{CATALOG}.{SCHEMA}.fuel_transactions")
    cust  = spark.table(f"{CATALOG}.{SCHEMA}.customers")
    store = spark.table(f"{CATALOG}.{SCHEMA}.store_purchases")
    return fuel, cust, store


# --------------------------------------------------------------------------- #
# 2. Build target label
#    went_to_store = 1 if this fuel_txn_id has any store line item
# --------------------------------------------------------------------------- #
def build_label(fuel, store):
    visited = (
        store.select("fuel_txn_id")
             .distinct()
             .withColumn("went_to_store", F.lit(1))
    )
    return fuel.join(visited, on="fuel_txn_id", how="left").fillna(0, subset=["went_to_store"])


# --------------------------------------------------------------------------- #
# 3. Feature engineering
# --------------------------------------------------------------------------- #
def build_features(labeled, cust):
    # -- Join customer profile --
    df = labeled.join(
        cust.select(
            "customer_id", "loyalty_member", "loyalty_tier",
            "member_since", "preferred_fuel",
        ),
        on="customer_id", how="left",
    )

    # -- Temporal features --
    df = (df
        .withColumn("hour_of_day",   F.hour("transaction_ts"))
        .withColumn("day_of_week",   F.dayofweek("transaction_ts"))   # 1=Sun … 7=Sat
        .withColumn("month",         F.month("transaction_ts"))
        .withColumn("is_weekend",    (F.dayofweek("transaction_ts").isin(1, 7)).cast("int"))
        .withColumn("is_morning",    (F.hour("transaction_ts").between(6, 11)).cast("int"))
        .withColumn("is_lunch",      (F.hour("transaction_ts").between(11, 14)).cast("int"))
        .withColumn("is_evening",    (F.hour("transaction_ts").between(17, 21)).cast("int"))
    )

    # -- Loyalty tenure (days as member at time of visit) --
    df = df.withColumn(
        "member_tenure_days",
        F.when(
            F.col("loyalty_member") == True,
            F.datediff(F.col("transaction_ts"), F.col("member_since")),
        ).otherwise(0),
    )

    # -- Preferred fuel match --
    df = df.withColumn(
        "preferred_fuel_match",
        (F.col("fuel_type") == F.col("preferred_fuel")).cast("int"),
    )

    # -- Pump proximity proxy: pumps 1-3 are closest to store entrance --
    df = df.withColumn(
        "near_entrance_pump",
        (F.col("pump_number") <= 3).cast("int"),
    )

    # ------------------------------------------------------------------ #
    # Historical behavioral features (computed BEFORE current transaction
    # using a window to avoid data leakage)
    # ------------------------------------------------------------------ #
    w_hist = (
        Window.partitionBy("customer_id")
              .orderBy("transaction_ts")
              .rowsBetween(Window.unboundedPreceding, -1)
    )

    df = (df
        .withColumn("hist_visit_count",      F.count("fuel_txn_id").over(w_hist))
        .withColumn("hist_avg_gallons",      F.avg("gallons").over(w_hist))
        .withColumn("hist_avg_cost",         F.avg("total_fuel_cost").over(w_hist))
        .withColumn("hist_total_loyalty_pts",F.sum("loyalty_points_earned").over(w_hist))
        .withColumn("hist_store_visits",     F.sum("went_to_store").over(w_hist))
        .fillna(0, subset=[
            "hist_visit_count", "hist_avg_gallons", "hist_avg_cost",
            "hist_total_loyalty_pts", "hist_store_visits",
        ])
    )

    # Historical store conversion rate for this customer
    df = df.withColumn(
        "hist_store_rate",
        F.when(F.col("hist_visit_count") > 0,
               F.col("hist_store_visits") / F.col("hist_visit_count"))
         .otherwise(0.0),
    )

    # -- Boolean → int --
    df = df.withColumn("is_loyalty_member", F.col("loyalty_member").cast("int"))

    # -- Loyalty tier ordinal (None → 0, Bronze → 1 … Platinum → 4) --
    df = df.withColumn(
        "loyalty_tier",
        F.when(F.col("loyalty_tier") == "Platinum", "Platinum")
         .when(F.col("loyalty_tier") == "Gold",     "Gold")
         .when(F.col("loyalty_tier") == "Silver",   "Silver")
         .when(F.col("loyalty_tier") == "Bronze",   "Bronze")
         .otherwise("None"),
    )

    return df


# --------------------------------------------------------------------------- #
# 4. Select final feature columns
# --------------------------------------------------------------------------- #
FEATURE_COLS = [
    # Transaction
    "fuel_type", "gallons", "price_per_gallon", "total_fuel_cost",
    "payment_method", "pump_number", "loyalty_points_earned",
    # Time
    "hour_of_day", "day_of_week", "month",
    "is_weekend", "is_morning", "is_lunch", "is_evening",
    # Customer profile
    "is_loyalty_member", "loyalty_tier", "member_tenure_days",
    "preferred_fuel", "preferred_fuel_match", "near_entrance_pump",
    # History (leak-free)
    "hist_visit_count", "hist_avg_gallons", "hist_avg_cost",
    "hist_total_loyalty_pts", "hist_store_rate",
]

TARGET_COL = "went_to_store"


def to_pandas(df):
    cols = FEATURE_COLS + [TARGET_COL, "transaction_ts", "fuel_txn_id"]
    return df.select(cols).toPandas()


# --------------------------------------------------------------------------- #
# 5. Encode categoricals
# --------------------------------------------------------------------------- #
def encode(X_train: pd.DataFrame, X_test: pd.DataFrame):
    enc = OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)
    X_train = X_train.copy()
    X_test  = X_test.copy()
    X_train[CAT_FEATURES] = enc.fit_transform(X_train[CAT_FEATURES])
    X_test[CAT_FEATURES]  = enc.transform(X_test[CAT_FEATURES])
    return X_train, X_test, enc


# --------------------------------------------------------------------------- #
# 6. Train + MLflow run
# --------------------------------------------------------------------------- #
def train_and_log(X_train, y_train, X_test, y_test, enc):
    mlflow.set_experiment(EXPERIMENT_NAME)

    with mlflow.start_run(run_name="xgb_store_conversion") as run:

        mlflow.log_params(XGB_PARAMS)
        mlflow.log_param("train_size",   len(X_train))
        mlflow.log_param("test_size",    len(X_test))
        mlflow.log_param("test_cutoff",  TEST_CUTOFF)
        mlflow.log_param("feature_count", len(FEATURE_COLS))

        model = xgb.XGBClassifier(**XGB_PARAMS)
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=50,
        )

        # -- Metrics --
        y_pred      = model.predict(X_test)
        y_pred_prob = model.predict_proba(X_test)[:, 1]

        metrics = {
            "roc_auc":   round(roc_auc_score(y_test, y_pred_prob), 4),
            "f1":        round(f1_score(y_test, y_pred),           4),
            "precision": round(precision_score(y_test, y_pred),    4),
            "recall":    round(recall_score(y_test, y_pred),       4),
            "accuracy":  round(accuracy_score(y_test, y_pred),     4),
        }
        mlflow.log_metrics(metrics)

        print("\n=== Test Set Performance ===")
        for k, v in metrics.items():
            print(f"  {k:<12}: {v}")
        print("\nClassification Report:\n", classification_report(y_test, y_pred,
              target_names=["Pumps Only", "Goes to Store"]))
        print("Confusion Matrix:\n", confusion_matrix(y_test, y_pred))

        # -- SHAP global feature importance --
        explainer   = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_test)

        shap_df = pd.DataFrame({
            "feature":    FEATURE_COLS,
            "mean_abs_shap": np.abs(shap_values).mean(axis=0),
        }).sort_values("mean_abs_shap", ascending=False)

        print("\n=== Top Features Driving Store Visits (SHAP) ===")
        print(shap_df.to_string(index=False))

        # Log SHAP importance as CSV artifact
        shap_path = "/tmp/shap_importance.csv"
        shap_df.to_csv(shap_path, index=False)
        mlflow.log_artifact(shap_path, artifact_path="explainability")

        # -- Log feature importance from XGBoost gain --
        fi_df = pd.DataFrame({
            "feature": FEATURE_COLS,
            "gain":    list(model.get_booster().get_score(importance_type="gain").values())
                       if model.get_booster().get_score(importance_type="gain") else [0] * len(FEATURE_COLS),
        }).sort_values("gain", ascending=False)
        fi_path = "/tmp/xgb_feature_importance.csv"
        fi_df.to_csv(fi_path, index=False)
        mlflow.log_artifact(fi_path, artifact_path="explainability")

        # -- Log model with input signature --
        signature = infer_signature(X_train, y_pred_prob)
        mlflow.xgboost.log_model(
            model,
            artifact_path="model",
            signature=signature,
            registered_model_name=MODEL_NAME,
            input_example=X_train.head(5),
        )

        print(f"\nMLflow run ID : {run.info.run_id}")
        print(f"Model registered as: {MODEL_NAME}")

    return model, shap_df, metrics


# --------------------------------------------------------------------------- #
# 7. Promote model to Staging in the registry
# --------------------------------------------------------------------------- #
def promote_to_staging(model_name: str):
    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions(f"name='{model_name}'")
    latest   = max(versions, key=lambda v: int(v.version))
    client.transition_model_version_stage(
        name=model_name, version=latest.version, stage="Staging",
        archive_existing_versions=True,
    )
    print(f"Promoted {model_name} v{latest.version} → Staging")


# --------------------------------------------------------------------------- #
# 8. Batch inference — score all fuel transactions
# --------------------------------------------------------------------------- #
def batch_score(model, enc, feature_df: pd.DataFrame) -> pd.DataFrame:
    X = feature_df[FEATURE_COLS].copy()
    X[CAT_FEATURES] = enc.transform(X[CAT_FEATURES])
    probs = model.predict_proba(X)[:, 1]
    return feature_df[["fuel_txn_id"]].assign(
        store_visit_prob=probs,
        predicted_store_visit=(probs >= 0.5).astype(int),
    )


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    print("Loading data from Delta tables...")
    fuel, cust, store = load_tables()

    print("Building labels and features...")
    labeled = build_label(fuel, store)
    featured = build_features(labeled, cust)
    pdf = to_pandas(featured)

    # Temporal train/test split
    cutoff = pd.Timestamp(TEST_CUTOFF)
    train_df = pdf[pdf["transaction_ts"] < cutoff]
    test_df  = pdf[pdf["transaction_ts"] >= cutoff]

    X_train = train_df[FEATURE_COLS]
    y_train = train_df[TARGET_COL].astype(int)
    X_test  = test_df[FEATURE_COLS]
    y_test  = test_df[TARGET_COL].astype(int)

    print(f"Train : {len(X_train):,} rows  |  Test : {len(X_test):,} rows")
    print(f"Train store rate : {y_train.mean():.2%}  |  Test store rate : {y_test.mean():.2%}")

    X_train_enc, X_test_enc, enc = encode(X_train, X_test)

    print("\nTraining XGBoost model...")
    model, shap_df, metrics = train_and_log(X_train_enc, y_train, X_test_enc, y_test, enc)

    promote_to_staging(MODEL_NAME)

    # Score the full dataset for downstream use
    print("\nRunning batch inference on full dataset...")
    X_all = pdf[FEATURE_COLS].copy()
    X_all[CAT_FEATURES] = enc.transform(X_all[CAT_FEATURES])
    scores = batch_score(model, enc, pdf)
    scores_spark = spark.createDataFrame(scores)
    scores_spark.write.format("delta").mode("overwrite").saveAsTable(
        f"{CATALOG}.{SCHEMA}.store_visit_predictions"
    )
    print(f"Scored {len(scores):,} transactions → {CATALOG}.{SCHEMA}.store_visit_predictions")

    print("\n=== Top 10 Drivers of Store Visits ===")
    print(shap_df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
