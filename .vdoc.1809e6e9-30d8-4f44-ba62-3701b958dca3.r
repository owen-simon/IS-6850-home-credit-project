#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
# Load libraries
library(tidyverse)
library(tidyr)
library(dplyr)
library(gt)
library(caret)
library(pROC)
library(randomForest)
library(xgboost)
library(lightgbm)

data_dir <- "data-files"

# Read datasets
train <- read_csv(file.path(data_dir, "train_final.csv")) |>
  mutate(across(where(is.character), as.factor))

test <- read_csv(file.path(data_dir, "test_final.csv")) |>
  mutate(across(where(is.character), as.factor))
#
#
#
#
#
TARGET_dist <- train |> 
  count(TARGET) |>
  mutate(proportion = n / sum(n))

gt(TARGET_dist)
#
#
#
# Majority class predictions
baseline_predictions <- rep.int(0, nrow(train))
baseline_accuracy <- mean(baseline_predictions == train$TARGET)

# AUC Calculation
majority_class_prob <- TARGET_dist$proportion[TARGET_dist$TARGET == 0]
baseline_probs <- rep(majority_class_prob, nrow(train))
baseline_auc <- roc(train$TARGET, baseline_probs)$auc

# Create results summary
baseline_results <- tibble(
  Metric = c("Baseline Accuracy", "Baseline AUC"),
  Value = c(baseline_accuracy, as.numeric(baseline_auc))
)

gt(baseline_results)
#
#
#
#
#
#
#
#
# load the raw application training data
raw_app_train <- read_csv(file.path(data_dir, "application_train.csv")) |>
  mutate(across(where(is.character), as.factor))

# Get column names from original application dataset
application_cols <- colnames(raw_app_train)

# Identify which application columns exist in the modeling dataset
application_cols_in_train <- intersect(application_cols, colnames(train))

# Keep only application-based predictors
train_all <- train |>
  select(all_of(application_cols_in_train), -SK_ID_CURR) |>
  mutate(TARGET = factor(TARGET,
                         levels = c(0, 1),
                         labels = c("No", "Yes")))

# Set up 5-fold cross-validation
set.seed(42)
ctrl <- trainControl(
  method = "cv",
  number = 5,
  classProbs = TRUE,
  summaryFunction = twoClassSummary,
  savePredictions = "final"
)
#
#
#
#
#
#
#
set.seed(42)
model_lr_stepwise <- train(
  TARGET ~ .,
  data = train_all,
  method = "glmStepAIC",
  trControl = ctrl,
  metric = "ROC",
  trace = 0,
  preProcess = c("medianImpute", "center", "scale")
)
#
#
#
#
#
set.seed(42)
model_xgb <- train(
  TARGET ~ .,
  data = train_all,
  method = "xgbTree",
  trControl = ctrl,
  metric = "ROC",
  tuneGrid = expand.grid(
    nrounds = 100,
    max_depth = 3,
    eta = 0.1,
    gamma = 0,
    colsample_bytree = 0.8,
    min_child_weight = 1,
    subsample = 0.8
  ),
  verbose = FALSE
)
#
#
#
#
#
set.seed(42)
model_lasso <- train(
  TARGET ~ .,
  data = train_preprocessed,
  method = "glmnet",
  family = "binomial",
  trControl = ctrl,
  metric = "ROC",
  tuneGrid = expand.grid(
    alpha = 1,
    lambda = seq(0.0001, 0.01, length = 10)
  )
)
#
#
#
#
#
set.seed(42)
model_rf <- train(
  TARGET ~ .,
  data = train_all,
  method = "rf",
  trControl = ctrl,
  metric = "ROC",
  tuneGrid = data.frame(mtry = c(15, 25, 35)),
  ntree = 100
)
#
#
#
#
#
set.seed(42)
model_lgbm <- train(
  TARGET ~ .,
  data = train_all,
  method = "lightgbm",
  trControl = ctrl,
  metric = "ROC",
  tuneGrid = expand.grid(
    num_leaves = 31,
    num_iterations = 100,
    learning_rate = 0.1
  ),
  verbose = -1
)
#
#
#
#
#
# Compare models
model_comparison <- resamples(list(
  "Stepwise Logistic Regression" = model_lr_stepwise,
  "XGBoost" = model_xgb,
  "LASSO" = model_lasso,
  "Random Forest" = model_rf,
  "LightGBM" = model_lgbm
))

summary(model_comparison)

# Visualize comparison
dotplot(model_comparison)
#
#
#
