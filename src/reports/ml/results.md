# Результати етапу машинного навчання (згенеровано автоматично)

Файл перезаписується скриптом `uv run -m src.ml.run`; інтерпретація — у `README.md` поруч.

## Етапи конвеєра попередньої обробки (Spark ML PipelineModel)

```
1. ImputerModel: uid=Imputer_59f4fceea7cf, strategy=median, missingValue=NaN, numInputCols=24, numOutputCols=24
2. VectorAssembler_19f30a0591a8
3. StandardScalerModel: uid=StandardScaler_2b139a4de77a, numFeatures=24, withMean=true, withStd=true
```

## Задача `regression`

### Розбиття на вибірки

| split | rows | mean_log_fans |
| --- | --- | --- |
| train | 1391658 | 0.2700 |
| validation | 298044 | 0.2728 |
| test | 298195 | 0.2702 |

### Порівняння моделей на тестовій вибірці

| model | rmse | r2 | mae | mae_fans | rmse_fans | rmse_95ci | r2_95ci | fit_seconds | best_params |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Baseline: train mean | 0.6696 | -0.0000 | 0.4290 | 1.6231 | 14.6968 | — | — | — | — |
| Baseline: 0 fans (median) | 0.7221 | -0.1629 | 0.2702 | 1.4408 | 14.7239 | — | — | — | — |
| LinearRegression | 0.3142 | 0.7799 | 0.1844 | 0.8356 | 9.2105 | [0.312; 0.316] | [0.777; 0.783] | 5.4000 | {"regParam": 0.001} |
| FMRegressor | 0.3101 | 0.7856 | 0.1798 | 0.8381 | 21.8457 | [0.308; 0.312] | [0.782; 0.790] | 166.9000 | {"stepSize": 0.01} |
| GBTRegressor | 0.2974 | 0.8027 | 0.1667 | 0.7379 | 9.5372 | [0.296; 0.299] | [0.800; 0.806] | 73.5000 | {"maxDepth": 5, "maxIter": 100} |

### Train / validation / test обраних моделей

| model | train_rmse | val_rmse | test_rmse | train_r2 | val_r2 | test_r2 |
| --- | --- | --- | --- | --- | --- | --- |
| LinearRegression | 0.3150 | 0.3170 | 0.3142 | 0.7782 | 0.7794 | 0.7799 |
| FMRegressor | 0.3114 | 0.3137 | 0.3101 | 0.7833 | 0.7840 | 0.7856 |
| GBTRegressor | 0.2953 | 0.2994 | 0.2974 | 0.8050 | 0.8032 | 0.8027 |

### Підбір гіперпараметрів (усі спроби)

| model | params | fit_seconds | search_seconds | train_rmse | val_rmse | train_r2 | val_r2 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| LinearRegression | {"regParam": 0.001} | 5.4000 | 5.4000 | 0.3150 | 0.3170 | 0.7782 | 0.7794 |
| LinearRegression | {"regParam": 0.01} | 3.5800 | 3.5800 | 0.3151 | 0.3171 | 0.7781 | 0.7793 |
| LinearRegression | {"regParam": 0.1} | 2.9700 | 2.9700 | 0.3165 | 0.3185 | 0.7761 | 0.7773 |
| FMRegressor | {"stepSize": 0.001} | 171.0800 | 171.0800 | 0.3287 | 0.3313 | 0.7585 | 0.7591 |
| FMRegressor | {"stepSize": 0.01} | 166.9000 | 166.9000 | 0.3114 | 0.3137 | 0.7833 | 0.7840 |
| FMRegressor | {"stepSize": 0.1} | 186.8800 | 186.8800 | 3.6329 | 3.7427 | -28.4980 | -29.7473 |
| GBTRegressor | {"maxDepth": 3, "maxIter": 100} | 54.9800 | 54.9800 | 0.2993 | 0.3011 | 0.7998 | 0.8010 |
| GBTRegressor | {"maxDepth": 5, "maxIter": 100} | 73.5300 | 73.5300 | 0.2953 | 0.2994 | 0.8050 | 0.8032 |

### Найважливіші ознаки моделей

| model | rank | feature | value |
| --- | --- | --- | --- |
| LinearRegression | 1 | log_cool | 0.1473 |
| LinearRegression | 2 | log_funny | 0.0734 |
| LinearRegression | 3 | log_compliments_total | 0.0731 |
| LinearRegression | 4 | log_compliment_photos | 0.0597 |
| LinearRegression | 5 | log_friends | 0.0549 |
| LinearRegression | 6 | log_compliment_hot | 0.0544 |
| LinearRegression | 7 | n_elite_years | 0.0525 |
| LinearRegression | 8 | log_compliment_plain | 0.0507 |
| LinearRegression | 9 | is_elite | 0.0406 |
| LinearRegression | 10 | log_compliment_cool | 0.0372 |
| LinearRegression | 11 | log_compliment_writer | 0.0363 |
| LinearRegression | 12 | log_compliment_list | -0.0218 |
| LinearRegression | 13 | log_compliment_more | 0.0114 |
| LinearRegression | 14 | log_compliment_note | 0.0113 |
| LinearRegression | 15 | log_compliment_cute | 0.0083 |
| FMRegressor | 1 | compliments_per_review | -0.0963 |
| FMRegressor | 2 | log_cool | 0.0864 |
| FMRegressor | 3 | log_compliments_total | 0.0692 |
| FMRegressor | 4 | log_funny | 0.0651 |
| FMRegressor | 5 | log_friends | 0.0582 |
| FMRegressor | 6 | is_elite | 0.0485 |
| FMRegressor | 7 | log_useful | 0.0445 |
| FMRegressor | 8 | log_compliment_plain | 0.0425 |
| FMRegressor | 9 | funny_per_review | -0.0403 |
| FMRegressor | 10 | log_compliment_cool | 0.0401 |
| FMRegressor | 11 | log_compliment_writer | 0.0383 |
| FMRegressor | 12 | cool_per_review | -0.0382 |
| FMRegressor | 13 | log_compliment_hot | 0.0369 |
| FMRegressor | 14 | log_compliment_photos | 0.0345 |
| FMRegressor | 15 | log_compliment_list | -0.0312 |
| GBTRegressor | 1 | log_cool | 0.6142 |
| GBTRegressor | 2 | log_compliment_plain | 0.0770 |
| GBTRegressor | 3 | log_friends | 0.0691 |
| GBTRegressor | 4 | log_compliments_total | 0.0501 |
| GBTRegressor | 5 | log_useful | 0.0311 |
| GBTRegressor | 6 | log_compliment_writer | 0.0237 |
| GBTRegressor | 7 | log_compliment_photos | 0.0206 |
| GBTRegressor | 8 | n_elite_years | 0.0188 |
| GBTRegressor | 9 | compliments_per_review | 0.0156 |
| GBTRegressor | 10 | log_review_count | 0.0150 |
| GBTRegressor | 11 | log_compliment_hot | 0.0091 |
| GBTRegressor | 12 | cool_per_review | 0.0086 |
| GBTRegressor | 13 | years_on_yelp | 0.0077 |
| GBTRegressor | 14 | log_funny | 0.0073 |
| GBTRegressor | 15 | log_compliment_profile | 0.0066 |

### Крива навчання найкращої моделі

| train_fraction | train_rows | fit_seconds | train_rmse | train_r2 | train_mae | train_mae_fans | train_rmse_fans | val_rmse | val_r2 | val_mae | val_mae_fans | val_rmse_fans |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0.0143 | 19732 | 23.9300 | 0.2399 | 0.8719 | 0.1374 | 0.4017 | 1.8345 | 0.3169 | 0.7796 | 0.1738 | 0.9187 | 26.0631 |
| 0.0429 | 59639 | 28.4400 | 0.2705 | 0.8392 | 0.1540 | 0.5678 | 5.4184 | 0.3091 | 0.7902 | 0.1708 | 0.9129 | 26.1395 |
| 0.1000 | 138902 | 32.2600 | 0.2838 | 0.8211 | 0.1609 | 0.6038 | 5.6400 | 0.3036 | 0.7977 | 0.1691 | 0.8575 | 25.7556 |
| 0.2429 | 337961 | 45.2300 | 0.2896 | 0.8128 | 0.1632 | 0.6651 | 7.0302 | 0.3003 | 0.8021 | 0.1675 | 0.8356 | 25.5508 |
| 0.5000 | 696313 | 51.5000 | 0.2938 | 0.8075 | 0.1648 | 0.6989 | 8.6542 | 0.2998 | 0.8027 | 0.1672 | 0.8246 | 25.4657 |
| 1.0000 | 1391658 | 73.5300 | 0.2953 | 0.8050 | 0.1655 | 0.7105 | 9.2257 | 0.2994 | 0.8032 | 0.1672 | 0.8292 | 25.3311 |

### Парний bootstrap: дві найкращі моделі на тесті

```json
{
  "a": "GBTRegressor",
  "b": "FMRegressor",
  "metric": "rmse",
  "diff": -0.012648280265255996,
  "ci_low": -0.013729324860701157,
  "ci_high": -0.011530205815560001,
  "share_a_better": 1.0
}
```


## Задача `classification`

### Розбиття на вибірки

| split | rows | mean_is_elite |
| --- | --- | --- |
| train | 1391658 | 0.0458 |
| validation | 298044 | 0.0462 |
| test | 298195 | 0.0458 |

### Порівняння моделей на тестовій вибірці

| model | accuracy | precision | recall | f1 | threshold | f1_95ci | pr_auc | pr_auc_95ci | roc_auc | fit_seconds | best_params |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Baseline: majority (nobody elite) | 0.9542 | 0.0000 | 0.0000 | 0.0000 | — | — | — | — | — | — | — |
| Baseline: review_count >= 88 | 0.9682 | 0.6328 | 0.7304 | 0.6781 | — | — | — | — | — | — | — |
| LogisticRegression | 0.9830 | 0.8122 | 0.8177 | 0.8150 | 0.3700 | [0.810; 0.821] | 0.8876 | [0.882; 0.893] | 0.9937 | 4.6000 | {"regParam": 0.001} |
| RandomForestClassifier | 0.9841 | 0.8212 | 0.8340 | 0.8276 | 0.4200 | [0.823; 0.832] | 0.9065 | [0.902; 0.911] | 0.9944 | 121.4000 | {"numTrees": 60, "maxDepth": 14} |
| MultilayerPerceptronClassifier | 0.9845 | 0.8270 | 0.8373 | 0.8321 | 0.4300 | [0.828; 0.838] | 0.9141 | [0.910; 0.918] | 0.9949 | 61.5000 | {"hidden": [32, 16]} |

### Train / validation / test обраних моделей

| model | train_pr_auc | val_pr_auc | test_pr_auc | train_roc_auc | val_roc_auc | test_roc_auc |
| --- | --- | --- | --- | --- | --- | --- |
| LogisticRegression | 0.8876 | 0.8870 | 0.8876 | 0.9936 | 0.9934 | 0.9937 |
| RandomForestClassifier | 0.9493 | 0.9077 | 0.9065 | 0.9971 | 0.9944 | 0.9944 |
| MultilayerPerceptronClassifier | 0.9142 | 0.9138 | 0.9141 | 0.9949 | 0.9948 | 0.9949 |

### Підбір гіперпараметрів (усі спроби)

| model | params | fit_seconds | search_seconds | train_pr_auc | val_pr_auc | train_f1@0.5 | val_f1@0.5 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| LogisticRegression | {"regParam": 0.001} | 4.5900 | 4.5900 | 0.8876 | 0.8870 | 0.8032 | 0.8024 |
| LogisticRegression | {"regParam": 0.01} | 2.9000 | 2.9000 | 0.8725 | 0.8713 | 0.7740 | 0.7759 |
| LogisticRegression | {"regParam": 0.1} | 2.1900 | 2.1900 | 0.8428 | 0.8424 | 0.6942 | 0.6947 |
| RandomForestClassifier | {"numTrees": 20, "maxDepth": 8} | 8.5300 | 8.5300 | 0.8889 | 0.8858 | 0.8115 | 0.8077 |
| RandomForestClassifier | {"numTrees": 60, "maxDepth": 8} | 30.0300 | 30.0300 | 0.8911 | 0.8873 | 0.8135 | 0.8092 |
| RandomForestClassifier | {"numTrees": 60, "maxDepth": 14} | 121.4200 | 121.4200 | 0.9493 | 0.9077 | 0.8773 | 0.8283 |
| MultilayerPerceptronClassifier | {"hidden": [16]} | 26.8300 | 26.8300 | 0.9127 | 0.9128 | 0.8292 | 0.8318 |
| MultilayerPerceptronClassifier | {"hidden": [32, 16]} | 61.4600 | 61.4600 | 0.9142 | 0.9138 | 0.8305 | 0.8320 |
| MultilayerPerceptronClassifier | {"hidden": [64, 32]} | 140.1700 | 140.1700 | 0.9136 | 0.9128 | 0.8298 | 0.8307 |

### Найважливіші ознаки моделей

| model | rank | feature | value |
| --- | --- | --- | --- |
| LogisticRegression | 1 | log_compliments_total | 1.2396 |
| LogisticRegression | 2 | log_review_count | 0.9561 |
| LogisticRegression | 3 | years_on_yelp | -0.7812 |
| LogisticRegression | 4 | log_cool | 0.6507 |
| LogisticRegression | 5 | average_stars | 0.5525 |
| LogisticRegression | 6 | log_useful | 0.4545 |
| LogisticRegression | 7 | log_funny | -0.3190 |
| LogisticRegression | 8 | useful_per_review | -0.3152 |
| LogisticRegression | 9 | log_friends | 0.2860 |
| LogisticRegression | 10 | log_compliment_cool | 0.2741 |
| LogisticRegression | 11 | log_compliment_plain | -0.2408 |
| LogisticRegression | 12 | log_fans | 0.2194 |
| LogisticRegression | 13 | log_compliment_cute | -0.1513 |
| LogisticRegression | 14 | funny_per_review | -0.1337 |
| LogisticRegression | 15 | log_compliment_writer | 0.1271 |
| RandomForestClassifier | 1 | log_compliments_total | 0.2607 |
| RandomForestClassifier | 2 | log_cool | 0.1308 |
| RandomForestClassifier | 3 | log_fans | 0.1100 |
| RandomForestClassifier | 4 | log_compliment_cool | 0.0872 |
| RandomForestClassifier | 5 | log_compliment_writer | 0.0855 |
| RandomForestClassifier | 6 | log_review_count | 0.0628 |
| RandomForestClassifier | 7 | log_compliment_note | 0.0507 |
| RandomForestClassifier | 8 | years_on_yelp | 0.0441 |
| RandomForestClassifier | 9 | log_compliment_plain | 0.0365 |
| RandomForestClassifier | 10 | log_compliment_hot | 0.0241 |
| RandomForestClassifier | 11 | average_stars | 0.0184 |
| RandomForestClassifier | 12 | log_compliment_photos | 0.0132 |
| RandomForestClassifier | 13 | funny_per_review | 0.0125 |
| RandomForestClassifier | 14 | log_friends | 0.0121 |
| RandomForestClassifier | 15 | useful_per_review | 0.0100 |

### Класифікатори при порозі 0,5 (тест)

| model | accuracy | precision | recall | f1 |
| --- | --- | --- | --- | --- |
| LogisticRegression | 0.9830 | 0.8547 | 0.7575 | 0.8031 |
| RandomForestClassifier | 0.9842 | 0.8490 | 0.7967 | 0.8220 |
| MultilayerPerceptronClassifier | 0.9847 | 0.8508 | 0.8089 | 0.8293 |

### Крива навчання найкращої моделі

| train_fraction | train_rows | fit_seconds | train_accuracy | train_precision | train_recall | train_f1 | train_macro_f1 | train_weighted_f1 | train_tp | train_fp | train_tn | train_fn | train_roc_auc | train_pr_auc | val_accuracy | val_precision | val_recall | val_f1 | val_macro_f1 | val_weighted_f1 | val_tp | val_fp | val_tn | val_fn | val_roc_auc | val_pr_auc |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0.0143 | 19732 | 4.2700 | 0.9901 | 0.9286 | 0.8515 | 0.8884 | 0.9416 | 0.9899 | 780.0000 | 60.0000 | 18756.0000 | 136.0000 | 0.9975 | 0.9606 | 0.9809 | 0.8264 | 0.7423 | 0.7821 | 0.8861 | 0.9804 | 10228.0000 | 2148.0000 | 282117.0000 | 3551.0000 | 0.9896 | 0.8524 |
| 0.0429 | 59639 | 6.3200 | 0.9860 | 0.8732 | 0.8190 | 0.8452 | 0.9190 | 0.9858 | 2272.0000 | 330.0000 | 56535.0000 | 502.0000 | 0.9957 | 0.9315 | 0.9839 | 0.8555 | 0.7847 | 0.8186 | 0.9051 | 0.9836 | 10812.0000 | 1826.0000 | 282439.0000 | 2967.0000 | 0.9939 | 0.8994 |
| 0.1000 | 138902 | 9.9700 | 0.9849 | 0.8584 | 0.8032 | 0.8299 | 0.9110 | 0.9847 | 5111.0000 | 843.0000 | 131696.0000 | 1252.0000 | 0.9949 | 0.9167 | 0.9844 | 0.8566 | 0.7961 | 0.8252 | 0.9085 | 0.9841 | 10970.0000 | 1837.0000 | 282428.0000 | 2809.0000 | 0.9945 | 0.9079 |
| 0.2429 | 337961 | 18.2600 | 0.9848 | 0.8569 | 0.8019 | 0.8285 | 0.9103 | 0.9846 | 12408.0000 | 2072.0000 | 320416.0000 | 3065.0000 | 0.9949 | 0.9143 | 0.9848 | 0.8574 | 0.8042 | 0.8299 | 0.9110 | 0.9845 | 11081.0000 | 1843.0000 | 282422.0000 | 2698.0000 | 0.9947 | 0.9109 |
| 0.5000 | 696313 | 34.0500 | 0.9850 | 0.8549 | 0.8094 | 0.8315 | 0.9118 | 0.9848 | 25854.0000 | 4388.0000 | 659981.0000 | 6090.0000 | 0.9949 | 0.9145 | 0.9849 | 0.8574 | 0.8086 | 0.8323 | 0.9122 | 0.9847 | 11142.0000 | 1853.0000 | 282412.0000 | 2637.0000 | 0.9948 | 0.9129 |
| 1.0000 | 1391658 | 61.4600 | 0.9849 | 0.8529 | 0.8091 | 0.8305 | 0.9113 | 0.9847 | 51586.0000 | 8895.0000 | 1319008.0000 | 12169.0000 | 0.9949 | 0.9142 | 0.9848 | 0.8535 | 0.8115 | 0.8320 | 0.9120 | 0.9847 | 11182.0000 | 1920.0000 | 282345.0000 | 2597.0000 | 0.9948 | 0.9138 |

### Парний bootstrap: дві найкращі моделі на тесті

```json
{
  "a": "MultilayerPerceptronClassifier",
  "b": "RandomForestClassifier",
  "metric": "pr_auc",
  "diff": 0.007572747131063706,
  "ci_low": 0.005813861778107476,
  "ci_high": 0.009193076455691021,
  "share_a_better": 1.0
}
```

