# Результати етапу машинного навчання (згенеровано автоматично)

Файл перезаписується скриптом `uv run -m src.ml.run`; інтерпретація — у `README.md` поруч.

## Етапи конвеєра попередньої обробки (Spark ML PipelineModel)

```
1. ImputerModel: uid=Imputer_ad5268d12061, strategy=median, missingValue=NaN, numInputCols=70, numOutputCols=70
2. StringIndexerModel: uid=StringIndexer_3432d1358c3a, handleInvalid=keep, stringOrderType=frequencyDesc, numInputCols=5, numOutputCols=5
3. OneHotEncoderModel: uid=OneHotEncoder_fa2bd62ff15f, dropLast=true, handleInvalid=keep, numInputCols=5, numOutputCols=5
4. CountVectorizerModel: uid=CountVectorizer_e9c8a9347562, vocabularySize=60
5. VectorAssembler_361cba66b904
6. StandardScalerModel: uid=StandardScaler_1ac95084eb3e, numFeatures=167, withMean=false, withStd=true
```

## Задача `regression`

### Розбиття на вибірки

| split | rows | mean_stars |
| --- | --- | --- |
| train | 105191 | 3.5946 |
| validation | 22654 | 3.6040 |
| test | 22501 | 3.5994 |

### Порівняння моделей на тестовій вибірці

| model | rmse | r2 | mae | within_half_star | rmse_95ci | r2_95ci | fit_seconds | best_params |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Baseline: train mean | 0.9730 | -0.0000 | 0.7960 | 0.3846 | — | — | — | — |
| Baseline: mean by state x category | 0.9209 | 0.1041 | 0.7442 | 0.3945 | — | — | — | — |
| LinearRegression | 0.7757 | 0.3643 | 0.6100 | 0.4960 | [0.768; 0.783] | [0.355; 0.373] | 4.4000 | {"regParam": 0.0005, "elasticNetParam": 0.0} |
| DecisionTreeRegressor | 0.7885 | 0.3433 | 0.6136 | 0.5083 | [0.781; 0.797] | [0.331; 0.355] | 1.0000 | {"maxDepth": 10, "minInstancesPerNode": 20} |
| RandomForestRegressor | 0.7218 | 0.4496 | 0.5654 | 0.5299 | [0.715; 0.730] | [0.440; 0.458] | 84.8000 | {"numTrees": 60, "maxDepth": 15} |
| GBTRegressor | 0.6860 | 0.5029 | 0.5269 | 0.5770 | [0.679; 0.693] | [0.493; 0.512] | 68.4000 | {"maxDepth": 5, "stepSize": 0.1, "maxIter": 200} |

### Train / validation / test обраних моделей

| model | train_rmse | val_rmse | test_rmse | train_r2 | val_r2 | test_r2 |
| --- | --- | --- | --- | --- | --- | --- |
| LinearRegression | 0.7811 | 0.7759 | 0.7757 | 0.3592 | 0.3597 | 0.3643 |
| DecisionTreeRegressor | 0.7623 | 0.7926 | 0.7885 | 0.3897 | 0.3320 | 0.3433 |
| RandomForestRegressor | 0.5959 | 0.7234 | 0.7218 | 0.6270 | 0.4434 | 0.4496 |
| GBTRegressor | 0.6513 | 0.6907 | 0.6860 | 0.5545 | 0.4926 | 0.5029 |

### Підбір гіперпараметрів (усі спроби)

| model | params | fit_seconds | search_seconds | train_rmse | val_rmse | train_r2 | val_r2 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| LinearRegression | {"regParam": 0.0005, "elasticNetParam": 0.0} | 4.3500 | 4.3500 | 0.7811 | 0.7759 | 0.3592 | 0.3597 |
| LinearRegression | {"regParam": 0.0005, "elasticNetParam": 0.5} | 6.1700 | 6.1700 | 0.7812 | 0.7760 | 0.3590 | 0.3596 |
| LinearRegression | {"regParam": 0.0005, "elasticNetParam": 1.0} | 8.4000 | 8.4000 | 0.7813 | 0.7761 | 0.3589 | 0.3595 |
| LinearRegression | {"regParam": 0.005, "elasticNetParam": 0.0} | 1.5900 | 1.5900 | 0.7815 | 0.7763 | 0.3586 | 0.3591 |
| LinearRegression | {"regParam": 0.005, "elasticNetParam": 0.5} | 9.0900 | 9.0900 | 0.7830 | 0.7776 | 0.3560 | 0.3569 |
| LinearRegression | {"regParam": 0.005, "elasticNetParam": 1.0} | 3.7900 | 3.7900 | 0.7855 | 0.7801 | 0.3519 | 0.3529 |
| LinearRegression | {"regParam": 0.05, "elasticNetParam": 0.0} | 1.3100 | 1.3100 | 0.7857 | 0.7803 | 0.3516 | 0.3524 |
| LinearRegression | {"regParam": 0.05, "elasticNetParam": 0.5} | 1.5500 | 1.5500 | 0.8190 | 0.8137 | 0.2955 | 0.2959 |
| LinearRegression | {"regParam": 0.05, "elasticNetParam": 1.0} | 1.0000 | 1.0000 | 0.8527 | 0.8480 | 0.2362 | 0.2353 |
| DecisionTreeRegressor | {"maxDepth": 2, "minInstancesPerNode": 1} | 0.7400 | 0.7400 | 0.9239 | 0.9189 | 0.1034 | 0.1021 |
| DecisionTreeRegressor | {"maxDepth": 2, "minInstancesPerNode": 20} | 0.4000 | 0.4000 | 0.9239 | 0.9189 | 0.1034 | 0.1021 |
| DecisionTreeRegressor | {"maxDepth": 4, "minInstancesPerNode": 1} | 0.5400 | 0.5400 | 0.8642 | 0.8601 | 0.2156 | 0.2133 |
| DecisionTreeRegressor | {"maxDepth": 4, "minInstancesPerNode": 20} | 0.4700 | 0.4700 | 0.8642 | 0.8601 | 0.2156 | 0.2133 |
| DecisionTreeRegressor | {"maxDepth": 6, "minInstancesPerNode": 1} | 0.5600 | 0.5600 | 0.8274 | 0.8272 | 0.2809 | 0.2723 |
| DecisionTreeRegressor | {"maxDepth": 6, "minInstancesPerNode": 20} | 0.5700 | 0.5700 | 0.8274 | 0.8270 | 0.2809 | 0.2727 |
| DecisionTreeRegressor | {"maxDepth": 8, "minInstancesPerNode": 1} | 0.6700 | 0.6700 | 0.7938 | 0.8041 | 0.3381 | 0.3123 |
| DecisionTreeRegressor | {"maxDepth": 8, "minInstancesPerNode": 20} | 0.7000 | 0.7000 | 0.7946 | 0.8033 | 0.3368 | 0.3137 |
| DecisionTreeRegressor | {"maxDepth": 10, "minInstancesPerNode": 1} | 1.0600 | 1.0600 | 0.7553 | 0.7996 | 0.4008 | 0.3201 |
| DecisionTreeRegressor | {"maxDepth": 10, "minInstancesPerNode": 20} | 0.9600 | 0.9600 | 0.7623 | 0.7926 | 0.3897 | 0.3320 |
| DecisionTreeRegressor | {"maxDepth": 12, "minInstancesPerNode": 1} | 1.3000 | 1.3000 | 0.7028 | 0.8190 | 0.4813 | 0.2866 |
| DecisionTreeRegressor | {"maxDepth": 12, "minInstancesPerNode": 20} | 1.3400 | 1.3400 | 0.7305 | 0.7950 | 0.4396 | 0.3278 |
| DecisionTreeRegressor | {"maxDepth": 15, "minInstancesPerNode": 1} | 2.4200 | 2.4200 | 0.5938 | 0.8801 | 0.6297 | 0.1762 |
| DecisionTreeRegressor | {"maxDepth": 15, "minInstancesPerNode": 20} | 1.8100 | 1.8100 | 0.6936 | 0.8074 | 0.4947 | 0.3067 |
| RandomForestRegressor | {"numTrees": 10, "maxDepth": 12} | 3.9100 | 3.9100 | 0.6954 | 0.7473 | 0.4921 | 0.4061 |
| RandomForestRegressor | {"numTrees": 30, "maxDepth": 12} | 11.5800 | 11.5800 | 0.6872 | 0.7388 | 0.5039 | 0.4195 |
| RandomForestRegressor | {"numTrees": 60, "maxDepth": 12} | 24.8600 | 24.8600 | 0.6862 | 0.7378 | 0.5054 | 0.4211 |
| RandomForestRegressor | {"numTrees": 100, "maxDepth": 12} | 49.9400 | 49.9400 | 0.6851 | 0.7363 | 0.5070 | 0.4235 |
| RandomForestRegressor | {"numTrees": 60, "maxDepth": 6} | 7.7300 | 7.7300 | 0.8109 | 0.8081 | 0.3093 | 0.3055 |
| RandomForestRegressor | {"numTrees": 60, "maxDepth": 9} | 21.0000 | 21.0000 | 0.7548 | 0.7650 | 0.4016 | 0.3776 |
| RandomForestRegressor | {"numTrees": 60, "maxDepth": 15} | 84.8100 | 84.8100 | 0.5959 | 0.7234 | 0.6270 | 0.4434 |
| GBTRegressor | {"maxDepth": 3, "stepSize": 0.1, "maxIter": 200} | 47.5800 | 47.5800 | 0.7092 | 0.7113 | 0.4717 | 0.4619 |
| GBTRegressor | {"maxDepth": 5, "stepSize": 0.1, "maxIter": 200} | 68.4200 | 68.4200 | 0.6513 | 0.6907 | 0.5545 | 0.4926 |
| GBTRegressor | {"maxDepth": 7, "stepSize": 0.1, "maxIter": 199} | 93.0300 | 186.7900 | 0.5570 | 0.6923 | 0.6741 | 0.4903 |
| GBTRegressor | {"maxDepth": 5, "stepSize": 0.3, "maxIter": 126} | 40.8500 | 105.2700 | 0.6269 | 0.6991 | 0.5873 | 0.4802 |

### Топ-20 ознак найкращої моделі

| feature | group | importance |
| --- | --- | --- |
| avg_review_cool | reviews | 0.1025 |
| avg_review_length | reviews | 0.1013 |
| weekly_open_hours | hours | 0.0752 |
| avg_review_funny | reviews | 0.0582 |
| avg_review_useful | reviews | 0.0558 |
| n_open_days | hours | 0.0416 |
| log_n_checkins | tips_checkins_photos | 0.0323 |
| category=fast food | categories | 0.0271 |
| n_attributes | attributes | 0.0229 |
| business_age_days | reviews | 0.0213 |
| attr_ByAppointmentOnly | attributes | 0.0212 |
| attr_RestaurantsDelivery | attributes | 0.0171 |
| category=active life | categories | 0.0160 |
| parking_street | attributes | 0.0160 |
| category=pets | categories | 0.0153 |
| category=real estate | categories | 0.0153 |
| attr_WheelchairAccessible | attributes | 0.0146 |
| n_categories | categories | 0.0139 |
| days_since_last_tip | recency | 0.0137 |
| days_since_last_review | recency | 0.0133 |

### Важливість груп ознак найкращої моделі

| group | share |
| --- | --- |
| reviews | 0.3391 |
| categories | 0.1976 |
| attributes | 0.1743 |
| hours | 0.1344 |
| recency | 0.0524 |
| tips_checkins_photos | 0.0456 |
| location | 0.0417 |
| popularity | 0.0122 |
| other_label | 0.0026 |

### Крива навчання найкращої моделі

| train_fraction | train_rows | fit_seconds | train_rmse | train_r2 | train_mae | train_within_half_star | val_rmse | val_r2 | val_mae | val_within_half_star |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0.0571 | 6109 | 32.3000 | 0.3561 | 0.8669 | 0.2687 | 0.8560 | 0.7823 | 0.3492 | 0.6025 | 0.5191 |
| 0.1000 | 10703 | 34.9600 | 0.4465 | 0.7917 | 0.3435 | 0.7673 | 0.7456 | 0.4088 | 0.5744 | 0.5358 |
| 0.2429 | 25825 | 40.9200 | 0.5666 | 0.6631 | 0.4362 | 0.6563 | 0.7131 | 0.4592 | 0.5499 | 0.5535 |
| 0.5000 | 52930 | 50.6600 | 0.6243 | 0.5932 | 0.4803 | 0.6146 | 0.6990 | 0.4803 | 0.5392 | 0.5612 |
| 1.0000 | 105191 | 64.7000 | 0.6513 | 0.5545 | 0.5009 | 0.5967 | 0.6907 | 0.4926 | 0.5318 | 0.5687 |

### Парний bootstrap: дві найкращі моделі на тесті

```json
{
  "a": "GBTRegressor",
  "b": "RandomForestRegressor",
  "metric": "rmse",
  "diff": -0.0358336223103497,
  "ci_low": -0.039409082114815724,
  "ci_high": -0.0320757129204945,
  "share_a_better": 1.0
}
```

### Абляція: лише ознаки профілю бізнесу

```json
{
  "model": "GBTRegressor",
  "params": {
    "maxDepth": 5,
    "stepSize": 0.1,
    "maxIter": 200
  },
  "n_features": 148,
  "fit_seconds": 64.77140545899965,
  "validation": {
    "rmse": 0.7928530617800831,
    "r2": 0.33147049324013855,
    "mae": 0.6218171508207473,
    "within_half_star": 0.49187781407257
  },
  "test": {
    "rmse": 0.7891311714471884,
    "r2": 0.3421518599767224,
    "mae": 0.617286519363843,
    "within_half_star": 0.4941558152970979
  }
}
```


## Задача `classification`

### Розбиття на вибірки

| split | rows | mean_is_closed |
| --- | --- | --- |
| train | 105191 | 0.2047 |
| validation | 22654 | 0.1994 |
| test | 22501 | 0.2046 |

### Порівняння моделей на тестовій вибірці

| model | accuracy | precision | recall | f1 | macro_f1 | weighted_f1 | roc_auc | pr_auc | f1_95ci | pr_auc_95ci | fit_seconds | best_params |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Baseline: majority (all open) | 0.7954 | 0.0000 | 0.0000 | 0.0000 | 0.4430 | 0.7048 | — | — | — | — | — | — |
| LogisticRegression | 0.7557 | 0.4403 | 0.7165 | 0.5454 | 0.6892 | 0.7741 | 0.8178 | 0.6244 | [0.535; 0.557] | [0.610; 0.638] | 3.4000 | {"regParam": 0.0005, "elasticNetParam": 0.0} |
| DecisionTreeClassifier | 0.7466 | 0.4246 | 0.6726 | 0.5206 | 0.6742 | 0.7649 | 0.7945 | 0.6024 | [0.510; 0.531] | [0.587; 0.615] | 1.6000 | {"maxDepth": 15, "minInstancesPerNode": 20} |
| RandomForestClassifier | 0.8363 | 0.5969 | 0.6157 | 0.6061 | 0.7514 | 0.8373 | 0.8463 | 0.6838 | [0.593; 0.617] | [0.671; 0.696] | 16.5000 | {"numTrees": 60, "maxDepth": 15} |
| GBTClassifier | 0.8197 | 0.5458 | 0.7071 | 0.6161 | 0.7491 | 0.8277 | 0.8617 | 0.7188 | [0.605; 0.627] | [0.707; 0.730] | 91.1000 | {"maxDepth": 7, "stepSize": 0.1, "maxIter": 198} |

### Train / validation / test обраних моделей

| model | train_f1 | val_f1 | test_f1 | train_pr_auc | val_pr_auc | test_pr_auc |
| --- | --- | --- | --- | --- | --- | --- |
| LogisticRegression | 0.5422 | 0.5456 | 0.5454 | 0.6181 | 0.6244 | 0.6244 |
| DecisionTreeClassifier | 0.6209 | 0.5227 | 0.5206 | 0.6871 | 0.6055 | 0.6024 |
| RandomForestClassifier | 0.7386 | 0.6051 | 0.6061 | 0.8312 | 0.6818 | 0.6838 |
| GBTClassifier | 0.7467 | 0.6151 | 0.6161 | 0.8561 | 0.7192 | 0.7188 |

### Підбір гіперпараметрів (усі спроби)

| model | params | fit_seconds | search_seconds | train_f1 | val_f1 | train_pr_auc | val_pr_auc |
| --- | --- | --- | --- | --- | --- | --- | --- |
| LogisticRegression | {"regParam": 0.0005, "elasticNetParam": 0.0} | 3.4100 | 3.4100 | 0.5422 | 0.5456 | 0.6181 | 0.6244 |
| LogisticRegression | {"regParam": 0.0005, "elasticNetParam": 0.5} | 10.0300 | 10.0300 | 0.5421 | 0.5457 | 0.6176 | 0.6241 |
| LogisticRegression | {"regParam": 0.0005, "elasticNetParam": 1.0} | 6.4900 | 6.4900 | 0.5417 | 0.5449 | 0.6171 | 0.6236 |
| LogisticRegression | {"regParam": 0.005, "elasticNetParam": 0.0} | 1.8600 | 1.8600 | 0.5424 | 0.5457 | 0.6164 | 0.6229 |
| LogisticRegression | {"regParam": 0.005, "elasticNetParam": 0.5} | 2.5300 | 2.5300 | 0.5400 | 0.5434 | 0.6087 | 0.6160 |
| LogisticRegression | {"regParam": 0.005, "elasticNetParam": 1.0} | 2.3900 | 2.3900 | 0.5364 | 0.5412 | 0.6016 | 0.6093 |
| LogisticRegression | {"regParam": 0.05, "elasticNetParam": 0.0} | 1.2400 | 1.2400 | 0.5372 | 0.5382 | 0.6039 | 0.6107 |
| LogisticRegression | {"regParam": 0.05, "elasticNetParam": 0.5} | 2.0200 | 2.0200 | 0.5024 | 0.5085 | 0.5357 | 0.5493 |
| LogisticRegression | {"regParam": 0.05, "elasticNetParam": 1.0} | 2.1900 | 2.1900 | 0.4713 | 0.4792 | 0.4574 | 0.4689 |
| DecisionTreeClassifier | {"maxDepth": 2, "minInstancesPerNode": 1} | 0.6300 | 0.6300 | 0.4173 | 0.4207 | 0.3285 | 0.3276 |
| DecisionTreeClassifier | {"maxDepth": 2, "minInstancesPerNode": 20} | 0.4700 | 0.4700 | 0.4173 | 0.4207 | 0.3285 | 0.3276 |
| DecisionTreeClassifier | {"maxDepth": 4, "minInstancesPerNode": 1} | 0.5900 | 0.5900 | 0.4796 | 0.4736 | 0.4277 | 0.4301 |
| DecisionTreeClassifier | {"maxDepth": 4, "minInstancesPerNode": 20} | 0.5300 | 0.5300 | 0.4796 | 0.4736 | 0.4277 | 0.4301 |
| DecisionTreeClassifier | {"maxDepth": 6, "minInstancesPerNode": 1} | 0.6500 | 0.6500 | 0.5154 | 0.5071 | 0.5283 | 0.5310 |
| DecisionTreeClassifier | {"maxDepth": 6, "minInstancesPerNode": 20} | 0.6600 | 0.6600 | 0.5152 | 0.5070 | 0.5260 | 0.5285 |
| DecisionTreeClassifier | {"maxDepth": 8, "minInstancesPerNode": 1} | 0.8100 | 0.8100 | 0.5497 | 0.5376 | 0.5923 | 0.5108 |
| DecisionTreeClassifier | {"maxDepth": 8, "minInstancesPerNode": 20} | 0.7600 | 0.7600 | 0.5476 | 0.5382 | 0.5814 | 0.5723 |
| DecisionTreeClassifier | {"maxDepth": 10, "minInstancesPerNode": 1} | 1.0500 | 1.0500 | 0.5774 | 0.5306 | 0.6482 | 0.5681 |
| DecisionTreeClassifier | {"maxDepth": 10, "minInstancesPerNode": 20} | 1.0200 | 1.0200 | 0.5681 | 0.5339 | 0.6209 | 0.5959 |
| DecisionTreeClassifier | {"maxDepth": 12, "minInstancesPerNode": 1} | 1.2500 | 1.2500 | 0.6320 | 0.5374 | 0.7161 | 0.5322 |
| DecisionTreeClassifier | {"maxDepth": 12, "minInstancesPerNode": 20} | 1.2000 | 1.2000 | 0.5979 | 0.5326 | 0.6574 | 0.6035 |
| DecisionTreeClassifier | {"maxDepth": 15, "minInstancesPerNode": 1} | 1.7100 | 1.7100 | 0.7093 | 0.5215 | 0.8198 | 0.4621 |
| DecisionTreeClassifier | {"maxDepth": 15, "minInstancesPerNode": 20} | 1.6500 | 1.6500 | 0.6209 | 0.5227 | 0.6871 | 0.6055 |
| RandomForestClassifier | {"numTrees": 10, "maxDepth": 12} | 2.0000 | 2.0000 | 0.6276 | 0.5745 | 0.7201 | 0.6515 |
| RandomForestClassifier | {"numTrees": 30, "maxDepth": 12} | 4.4700 | 4.4700 | 0.6453 | 0.5839 | 0.7362 | 0.6640 |
| RandomForestClassifier | {"numTrees": 60, "maxDepth": 12} | 8.8300 | 8.8300 | 0.6498 | 0.5914 | 0.7436 | 0.6687 |
| RandomForestClassifier | {"numTrees": 100, "maxDepth": 12} | 15.9100 | 15.9100 | 0.6512 | 0.5879 | 0.7428 | 0.6688 |
| RandomForestClassifier | {"numTrees": 60, "maxDepth": 6} | 3.0200 | 3.0200 | 0.5181 | 0.5176 | 0.5808 | 0.5887 |
| RandomForestClassifier | {"numTrees": 60, "maxDepth": 9} | 4.8300 | 4.8300 | 0.5749 | 0.5608 | 0.6550 | 0.6383 |
| RandomForestClassifier | {"numTrees": 60, "maxDepth": 15} | 16.5100 | 16.5100 | 0.7386 | 0.6051 | 0.8312 | 0.6818 |
| GBTClassifier | {"maxDepth": 3, "stepSize": 0.1, "maxIter": 200} | 46.8200 | 46.8200 | 0.5940 | 0.5864 | 0.6870 | 0.6786 |
| GBTClassifier | {"maxDepth": 5, "stepSize": 0.1, "maxIter": 200} | 67.4000 | 67.4000 | 0.6541 | 0.6031 | 0.7658 | 0.7114 |
| GBTClassifier | {"maxDepth": 7, "stepSize": 0.1, "maxIter": 198} | 91.0800 | 182.9600 | 0.7467 | 0.6151 | 0.8561 | 0.7192 |
| GBTClassifier | {"maxDepth": 5, "stepSize": 0.3, "maxIter": 200} | 65.4700 | 65.4700 | 0.7261 | 0.6140 | 0.8339 | 0.7156 |

### Топ-20 ознак найкращої моделі

| feature | group | importance |
| --- | --- | --- |
| business_age_days | reviews | 0.0612 |
| log_review_count | popularity | 0.0567 |
| avg_review_length | reviews | 0.0509 |
| weekly_open_hours | hours | 0.0364 |
| latitude | location | 0.0363 |
| n_attributes | attributes | 0.0356 |
| log_n_checkins | tips_checkins_photos | 0.0308 |
| longitude | location | 0.0288 |
| avg_review_useful | reviews | 0.0265 |
| stars | other_label | 0.0256 |
| avg_review_cool | reviews | 0.0252 |
| attr_RestaurantsDelivery | attributes | 0.0244 |
| log_n_tips | tips_checkins_photos | 0.0229 |
| avg_review_funny | reviews | 0.0215 |
| n_categories | categories | 0.0201 |
| hours_zero_format | hours | 0.0177 |
| attire=missing | attributes | 0.0177 |
| attr_HasTV | attributes | 0.0169 |
| log_n_photos | tips_checkins_photos | 0.0124 |
| attr_BusinessAcceptsCreditCards | attributes | 0.0119 |

### Важливість груп ознак найкращої моделі

| group | share |
| --- | --- |
| attributes | 0.3086 |
| reviews | 0.1853 |
| categories | 0.1682 |
| location | 0.0882 |
| tips_checkins_photos | 0.0872 |
| hours | 0.0802 |
| popularity | 0.0567 |
| other_label | 0.0256 |

### Крива навчання найкращої моделі

| train_fraction | train_rows | fit_seconds | train_accuracy | train_precision | train_recall | train_f1 | train_macro_f1 | train_weighted_f1 | train_tp | train_fp | train_tn | train_fn | train_roc_auc | train_pr_auc | val_accuracy | val_precision | val_recall | val_f1 | val_macro_f1 | val_weighted_f1 | val_tp | val_fp | val_tn | val_fn | val_roc_auc | val_pr_auc |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0.0571 | 6109 | 47.7000 | 0.9998 | 0.9992 | 1.0000 | 0.9996 | 0.9997 | 0.9998 | 1217.0000 | 1.0000 | 4891.0000 | 0.0000 | 1.0000 | 1.0000 | 0.8316 | 0.5871 | 0.5231 | 0.5533 | 0.7247 | 0.8278 | 2363.0000 | 1662.0000 | 16475.0000 | 2154.0000 | 0.8014 | 0.6166 |
| 0.1000 | 10703 | 50.7400 | 0.9948 | 0.9763 | 0.9991 | 0.9876 | 0.9921 | 0.9948 | 2228.0000 | 54.0000 | 8419.0000 | 2.0000 | 0.9999 | 0.9996 | 0.8300 | 0.5738 | 0.5729 | 0.5734 | 0.7336 | 0.8300 | 2588.0000 | 1922.0000 | 16215.0000 | 1929.0000 | 0.8188 | 0.6427 |
| 0.2429 | 25825 | 59.6500 | 0.9509 | 0.8335 | 0.9481 | 0.8871 | 0.9279 | 0.9520 | 4986.0000 | 996.0000 | 19570.0000 | 273.0000 | 0.9912 | 0.9704 | 0.8278 | 0.5597 | 0.6405 | 0.5974 | 0.7439 | 0.8321 | 2893.0000 | 2276.0000 | 15861.0000 | 1624.0000 | 0.8391 | 0.6812 |
| 0.5000 | 52930 | 72.6100 | 0.9108 | 0.7276 | 0.9038 | 0.8062 | 0.8741 | 0.9142 | 9815.0000 | 3674.0000 | 38396.0000 | 1045.0000 | 0.9696 | 0.9090 | 0.8236 | 0.5458 | 0.6878 | 0.6086 | 0.7474 | 0.8308 | 3107.0000 | 2586.0000 | 15551.0000 | 1410.0000 | 0.8504 | 0.6993 |
| 1.0000 | 105191 | 91.6500 | 0.8804 | 0.6588 | 0.8615 | 0.7467 | 0.8342 | 0.8859 | 18547.0000 | 9605.0000 | 74058.0000 | 2981.0000 | 0.9484 | 0.8561 | 0.8229 | 0.5426 | 0.7100 | 0.6151 | 0.7500 | 0.8312 | 3207.0000 | 2703.0000 | 15434.0000 | 1310.0000 | 0.8616 | 0.7192 |

### Ваги класів × поріг (найкраща модель)

| class_weights | threshold | threshold_kind | accuracy | precision | recall | f1 | macro_f1 | weighted_f1 | pr_auc |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| weighted | 0.5000 | 0.5 | 0.8197 | 0.5458 | 0.7071 | 0.6161 | 0.7491 | 0.8277 | 0.7188 |
| weighted | 0.6500 | tuned | 0.8574 | 0.6708 | 0.5946 | 0.6304 | 0.7710 | 0.8541 | 0.7188 |
| unweighted | 0.5000 | 0.5 | 0.8668 | 0.7907 | 0.4743 | 0.5929 | 0.7566 | 0.8534 | 0.7139 |
| unweighted | 0.3300 | tuned | 0.8571 | 0.6691 | 0.5966 | 0.6308 | 0.7711 | 0.8540 | 0.7139 |

### Парний bootstrap: дві найкращі моделі на тесті

```json
{
  "a": "GBTClassifier",
  "b": "RandomForestClassifier",
  "metric": "pr_auc",
  "diff": 0.034936059518483886,
  "ci_low": 0.028515594789272226,
  "ci_high": 0.04214210614359988,
  "share_a_better": 1.0
}
```

### Поріг класифікації, обраний на валідації

```json
{
  "threshold": 0.65,
  "test_at_0.5": {
    "accuracy": 0.8196969023598951,
    "precision": 0.545774647887324,
    "recall": 0.7071475124918531,
    "f1": 0.61606889372575,
    "macro_f1": 0.7491263591614086,
    "weighted_f1": 0.8277450429073344
  },
  "test_at_tuned": {
    "accuracy": 0.8573841162614995,
    "precision": 0.6708333333333333,
    "recall": 0.5946122094286335,
    "f1": 0.6304272716802948,
    "macro_f1": 0.7710356573715772,
    "weighted_f1": 0.8541158977060335
  }
}
```


## Задача `classification_recency`

### Розбиття на вибірки

| split | rows | mean_is_closed |
| --- | --- | --- |
| train | 105191 | 0.2047 |
| validation | 22654 | 0.1994 |
| test | 22501 | 0.2046 |

### Порівняння моделей на тестовій вибірці

| model | accuracy | precision | recall | f1 | macro_f1 | weighted_f1 | roc_auc | pr_auc | f1_95ci | pr_auc_95ci | fit_seconds | best_params |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Baseline: majority (all open) | 0.7954 | 0.0000 | 0.0000 | 0.0000 | 0.4430 | 0.7048 | — | — | — | — | — | — |
| Baseline: days since last review >= 1007 | 0.8801 | 0.7035 | 0.7156 | 0.7095 | 0.8170 | 0.8805 | — | — | — | — | — | — |
| LogisticRegression | 0.8974 | 0.7049 | 0.8573 | 0.7736 | 0.8537 | 0.9009 | 0.9470 | 0.8724 | [0.765; 0.783] | [0.865; 0.880] | 2.8000 | {"regParam": 0.0005, "elasticNetParam": 0.0} |
| DecisionTreeClassifier | 0.8876 | 0.6744 | 0.8716 | 0.7604 | 0.8435 | 0.8926 | 0.9503 | 0.8769 | [0.752; 0.771] | [0.869; 0.885] | 1.4000 | {"maxDepth": 15, "minInstancesPerNode": 20} |
| RandomForestClassifier | 0.9091 | 0.7369 | 0.8642 | 0.7955 | 0.8685 | 0.9117 | 0.9599 | 0.9014 | [0.787; 0.804] | [0.896; 0.908] | 13.8000 | {"numTrees": 60, "maxDepth": 15} |
| GBTClassifier | 0.9165 | 0.7578 | 0.8701 | 0.8101 | 0.8783 | 0.9186 | 0.9650 | 0.9149 | [0.802; 0.819] | [0.909; 0.921] | 100.5000 | {"maxDepth": 7, "stepSize": 0.1, "maxIter": 200} |

### Train / validation / test обраних моделей

| model | train_f1 | val_f1 | test_f1 | train_pr_auc | val_pr_auc | test_pr_auc |
| --- | --- | --- | --- | --- | --- | --- |
| LogisticRegression | 0.7759 | 0.7760 | 0.7736 | 0.8705 | 0.8728 | 0.8724 |
| DecisionTreeClassifier | 0.8132 | 0.7605 | 0.7604 | 0.9008 | 0.8770 | 0.8769 |
| RandomForestClassifier | 0.8523 | 0.7973 | 0.7955 | 0.9530 | 0.9003 | 0.9014 |
| GBTClassifier | 0.8874 | 0.8095 | 0.8101 | 0.9677 | 0.9141 | 0.9149 |

### Підбір гіперпараметрів (усі спроби)

| model | params | fit_seconds | search_seconds | train_f1 | val_f1 | train_pr_auc | val_pr_auc |
| --- | --- | --- | --- | --- | --- | --- | --- |
| LogisticRegression | {"regParam": 0.0005, "elasticNetParam": 0.0} | 2.7900 | 2.7900 | 0.7759 | 0.7760 | 0.8705 | 0.8728 |
| DecisionTreeClassifier | {"maxDepth": 15, "minInstancesPerNode": 20} | 1.4000 | 1.4000 | 0.8132 | 0.7605 | 0.9008 | 0.8770 |
| RandomForestClassifier | {"numTrees": 60, "maxDepth": 15} | 13.7900 | 13.7900 | 0.8523 | 0.7973 | 0.9530 | 0.9003 |
| GBTClassifier | {"maxDepth": 7, "stepSize": 0.1, "maxIter": 200} | 100.4800 | 100.4800 | 0.8874 | 0.8095 | 0.9677 | 0.9141 |

### Топ-20 ознак найкращої моделі

| feature | group | importance |
| --- | --- | --- |
| days_since_last_review | recency | 0.3544 |
| days_since_last_checkin | recency | 0.0658 |
| log_review_count | popularity | 0.0556 |
| business_age_days | reviews | 0.0393 |
| days_since_last_tip | recency | 0.0253 |
| log_n_checkins | tips_checkins_photos | 0.0226 |
| latitude | location | 0.0217 |
| stars | other_label | 0.0216 |
| avg_review_length | reviews | 0.0187 |
| share_reviews_last_year | recency | 0.0175 |
| avg_review_useful | reviews | 0.0167 |
| weekly_open_hours | hours | 0.0166 |
| n_attributes | attributes | 0.0164 |
| avg_review_cool | reviews | 0.0140 |
| longitude | location | 0.0138 |
| n_categories | categories | 0.0136 |
| avg_review_funny | reviews | 0.0129 |
| attr_ByAppointmentOnly | attributes | 0.0123 |
| log_n_tips | tips_checkins_photos | 0.0112 |
| n_open_days | hours | 0.0098 |

### Важливість груп ознак найкращої моделі

| group | share |
| --- | --- |
| recency | 0.4707 |
| categories | 0.1062 |
| attributes | 0.1020 |
| reviews | 0.1016 |
| location | 0.0570 |
| popularity | 0.0556 |
| tips_checkins_photos | 0.0486 |
| hours | 0.0367 |
| other_label | 0.0216 |

### Парний bootstrap: дві найкращі моделі на тесті

```json
{
  "a": "GBTClassifier",
  "b": "RandomForestClassifier",
  "metric": "pr_auc",
  "diff": 0.013566808757689675,
  "ci_low": 0.010343979904159948,
  "ci_high": 0.016488306209973987,
  "share_a_better": 1.0
}
```

### Поріг класифікації, обраний на валідації

```json
{
  "threshold": 0.72,
  "test_at_0.5": {
    "accuracy": 0.9165370427980979,
    "precision": 0.7578051087984863,
    "recall": 0.8700847273517271,
    "f1": 0.8100728155339806,
    "macro_f1": 0.8782949371284985,
    "weighted_f1": 0.9186048392039224
  },
  "test_at_tuned": {
    "accuracy": 0.9318252522110129,
    "precision": 0.8483541430192962,
    "recall": 0.8118618292417988,
    "f1": 0.8297069271758436,
    "macro_f1": 0.8935443565145207,
    "weighted_f1": 0.9312635077992507
  }
}
```

