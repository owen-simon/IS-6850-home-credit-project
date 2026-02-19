# ==============================
# Load packages
# ==============================
library(dplyr)
library(readr)
library(tidyr)

# ==============================
# Load datasets
# ==============================
data_dir <- "data-files"
dict <- read_csv(file.path(data_dir, "HomeCredit_columns_description.csv"))
train <- read_csv(file.path(data_dir, "application_train.csv"))
test  <- read_csv(file.path(data_dir, "application_test.csv"))
pos_cash <- read_csv(file.path(data_dir, "POS_CASH_balance.csv"))
bureau <- read_csv(file.path(data_dir, "bureau.csv"))
bureau_balance <- read_csv(file.path(data_dir, "bureau_balance.csv"))
cred_card <- read_csv(file.path(data_dir, "credit_card_balance.csv"))
installments <- read_csv(file.path(data_dir, "installments_payments.csv"))
prev_app <- read_csv(file.path(data_dir, "previous_application.csv"))

# ================================
# Safe min / max / sum functions
# ================================
safe_mean <- function(x, ...) {
  if (all(is.na(x))) NA_real_ else mean(x, na.rm = TRUE)
}

safe_sum <- function(x, ...) {
  if (all(is.na(x))) NA_real_ else sum(x, na.rm = TRUE)
}

safe_min <- function(x, ...) {
  if (all(is.na(x))) NA_real_ else min(x, na.rm = TRUE)
}

safe_max <- function(x, ...) {
  if (all(is.na(x))) NA_real_ else max(x, na.rm = TRUE)
}

# =============================================================================================
# POS_CASH_balance: Aggregate monthly loan data to loan-level first
# 
# Each previous POS loan (SK_ID_PREV) has multiple monthly snapshots.
# This step condenses the month-by-month data into meaningful loan-level metrics:
#   - CNT_INSTALMENT_mean / max → average / longest loan term
#   - CNT_INSTALMENT_FUTURE_mean / min → typical / smallest remaining installments
#   - SK_DPD / SK_DPD_DEF → mean, max, sum to capture late payment behavior
#   - ever delinquent flags → whether the loan was ever past due
# 
# Result: one row per loan, still grouped by SK_ID_CURR, ready for client-level aggregation.
# =============================================================================================
pos_prev_features <- pos_cash |>
  group_by(SK_ID_CURR, SK_ID_PREV) |>
  summarise(
    pos_MONTHS_BALANCE_count = n(),
    pos_MONTHS_BALANCE_min   = safe_min(MONTHS_BALANCE),
    
    pos_CNT_INSTALMENT_mean = safe_mean(CNT_INSTALMENT, na.rm = TRUE),
    pos_CNT_INSTALMENT_max  = safe_max(CNT_INSTALMENT),
    
    pos_CNT_INSTALMENT_FUTURE_mean = safe_mean(CNT_INSTALMENT_FUTURE, na.rm = TRUE),
    pos_CNT_INSTALMENT_FUTURE_min  = safe_min(CNT_INSTALMENT_FUTURE),
    
    pos_SK_DPD_mean = safe_mean(SK_DPD, na.rm = TRUE),
    pos_SK_DPD_max  = safe_max(SK_DPD),
    pos_SK_DPD_sum  = safe_sum(SK_DPD, na.rm = TRUE),
    
    pos_SK_DPD_DEF_mean = safe_mean(SK_DPD_DEF, na.rm = TRUE),
    pos_SK_DPD_DEF_max  = safe_max(SK_DPD_DEF),
    pos_SK_DPD_DEF_sum  = safe_sum(SK_DPD_DEF, na.rm = TRUE),
    
    .groups = "drop"
  ) |>
  mutate(
    pos_SK_DPD_ever     = as.integer(pos_SK_DPD_max > 0),
    pos_SK_DPD_DEF_ever = as.integer(pos_SK_DPD_DEF_max > 0)
  )

# ========================================================================================
# POS_CASH_balance: Aggregate loan-level features to client-level
#
# Now each client (SK_ID_CURR) may have multiple previous POS loans.
# This step summarizes across all loans to create client-level metrics:
#   - pos_SK_ID_PREV_count → number of previous POS loans
#   - mean / max / min / sum of loan-level metrics → typical, worst, and total behavior
#   - ever delinquent rate → fraction of loans with any late payment
# 
# Result: one row per client, ready to merge with other client-level tables.
# ========================================================================================
pos_cash_client <- pos_prev_features |>
  group_by(SK_ID_CURR) |>
  summarise(
    COUNT_POS_LOANS = n(),
    
    pos_CNT_INSTALMENT_mean = safe_mean(pos_CNT_INSTALMENT_mean, na.rm = TRUE),
    pos_CNT_INSTALMENT_max  = safe_max(pos_CNT_INSTALMENT_max),
    
    pos_CNT_INSTALMENT_FUTURE_mean = safe_mean(pos_CNT_INSTALMENT_FUTURE_mean, na.rm = TRUE),
    pos_CNT_INSTALMENT_FUTURE_min  = safe_min(pos_CNT_INSTALMENT_FUTURE_min),
    
    pos_SK_DPD_mean = safe_mean(pos_SK_DPD_mean, na.rm = TRUE),
    pos_SK_DPD_max  = safe_max(pos_SK_DPD_max),
    pos_SK_DPD_sum  = safe_sum(pos_SK_DPD_sum, na.rm = TRUE),
    
    pos_SK_DPD_DEF_mean = safe_mean(pos_SK_DPD_DEF_mean, na.rm = TRUE),
    pos_SK_DPD_DEF_max  = safe_max(pos_SK_DPD_DEF_max),
    pos_SK_DPD_DEF_sum  = safe_sum(pos_SK_DPD_DEF_sum, na.rm = TRUE),
    
    pos_SK_DPD_ever_rate     = safe_mean(pos_SK_DPD_ever, na.rm = TRUE),
    pos_SK_DPD_DEF_ever_rate = safe_mean(pos_SK_DPD_DEF_ever, na.rm = TRUE),
    
    .groups = "drop"
  )

# =============================================================================
# bureau_balance: Aggregate monthly CB loan data to credit-level first
#
# Each Credit Bureau credit (SK_BUREAU_ID) has multiple monthly snapshots.
# This step condenses month-by-month data into loan-level metrics:
#   - MONTHS_BALANCE_count / min → number of months observed, earliest month
#   - STATUS counts → how often credit was active, closed, overdue
#   - ever overdue flag → did this credit ever go past due
#
# Result: one row per Credit Bureau credit, ready to join with bureau.csv
# =============================================================================
bureau_prev_features <- bureau_balance |>
  group_by(SK_ID_BUREAU) |>
  summarise(
    bb_MONTHS_BALANCE_count = n(),
    bb_MONTHS_BALANCE_min   = safe_min(MONTHS_BALANCE),
    
    bb_status_C_count = sum(STATUS == "C", na.rm = TRUE),
    bb_status_X_count = sum(STATUS == "X", na.rm = TRUE),
    bb_status_0_count = sum(STATUS == "0", na.rm = TRUE),
    bb_status_1_count = sum(STATUS == "1", na.rm = TRUE),
    bb_status_2_count = sum(STATUS == "2", na.rm = TRUE),
    bb_status_3_count = sum(STATUS == "3", na.rm = TRUE),
    bb_status_4_count = sum(STATUS == "4", na.rm = TRUE),
    bb_status_5_count = sum(STATUS == "5", na.rm = TRUE),
    
    .groups = "drop"
  ) |>
  mutate(
    bb_ever_overdue = as.integer(bb_status_1_count + bb_status_2_count +
                                 bb_status_3_count + bb_status_4_count +
                                 bb_status_5_count > 0)
  )

# ============================================================================
# bureau: Aggregate credit-level data to client-level
#
# Each client (SK_ID_CURR) may have multiple previous Credit Bureau loans.
# This step combines bureau.csv and credit-level features from bureau_balance:
#   - Count of previous credits
#   - Mean / max / sum of credit amounts, overdue amounts, durations
#   - Fraction of credits ever overdue
#
# Result: one row per client, ready to merge with other client-level tables
# ============================================================================
bureau_client <- bureau |>
  left_join(bureau_prev_features, by = "SK_ID_BUREAU") |>
  group_by(SK_ID_CURR) |>
  summarise(
    COUNT_BUREAU_RECORDS     = n(),
    
    bureau_AMT_CREDIT_SUM_mean  = safe_mean(AMT_CREDIT_SUM, na.rm = TRUE),
    bureau_AMT_CREDIT_SUM_max   = safe_max(AMT_CREDIT_SUM),
    bureau_AMT_CREDIT_SUM_debt  = safe_sum(AMT_CREDIT_SUM_DEBT, na.rm = TRUE),
    
    bureau_days_credit_mean     = safe_mean(DAYS_CREDIT, na.rm = TRUE),
    bureau_days_credit_end_max  = safe_max(DAYS_CREDIT_ENDDATE),
    
    bureau_ever_overdue_rate = ifelse(all(is.na(bb_ever_overdue)), NA_real_,
         safe_mean(bb_ever_overdue, na.rm = TRUE)),
    
    .groups = "drop"
   ) |>
  mutate(
    bureau_DEBT_RATIO =
      bureau_AMT_CREDIT_SUM_debt /
      (bureau_AMT_CREDIT_SUM_mean + 1)
  )

# ============================================================================
# credit_card_balance: Aggregate monthly credit card data to credit-level
#
# Each previous credit card (SK_ID_PREV) has multiple monthly snapshots.
# This step condenses month-by-month data into loan-level metrics:
#   - MONTHS_BALANCE_count / min → number of months observed, earliest month
#   - AMT balances → mean / max / sum across months
#   - CNT drawings → mean / max / sum across months
#   - SK_DPD / SK_DPD_DEF → mean, max, sum to capture delinquency
#   - ever delinquent flags → whether the credit was ever past due
#
# Result: one row per credit card account, ready for client-level aggregation.
# ============================================================================
cc_prev_features <- cred_card |>
  group_by(SK_ID_CURR, SK_ID_PREV) |>
  summarise(
    cc_MONTHS_BALANCE_count = n(),
    cc_MONTHS_BALANCE_min   = safe_min(MONTHS_BALANCE),
    
    cc_AMT_BALANCE_mean     = safe_mean(AMT_BALANCE, na.rm = TRUE),
    cc_AMT_BALANCE_max      = safe_max(AMT_BALANCE),
    cc_AMT_BALANCE_sum      = safe_sum(AMT_BALANCE, na.rm = TRUE),
    
    cc_AMT_CREDIT_LIMIT_ACTUAL_mean = safe_mean(AMT_CREDIT_LIMIT_ACTUAL, na.rm = TRUE),
    cc_AMT_CREDIT_LIMIT_ACTUAL_max  = safe_max(AMT_CREDIT_LIMIT_ACTUAL),
    
    cc_AMT_PAYMENT_CURRENT_sum  = safe_sum(AMT_PAYMENT_CURRENT, na.rm = TRUE),
    cc_AMT_PAYMENT_TOTAL_CURRENT_sum = safe_sum(AMT_PAYMENT_TOTAL_CURRENT, na.rm = TRUE),
    
    cc_CNT_DRAWINGS_CURRENT_sum = safe_sum(CNT_DRAWINGS_CURRENT, na.rm = TRUE),
    cc_CNT_INSTALMENT_MATURE_CUM = safe_max(CNT_INSTALMENT_MATURE_CUM),
    
    cc_SK_DPD_mean     = safe_mean(SK_DPD, na.rm = TRUE),
    cc_SK_DPD_max      = safe_max(SK_DPD),
    cc_SK_DPD_sum      = safe_sum(SK_DPD, na.rm = TRUE),
    
    cc_SK_DPD_DEF_mean = safe_mean(SK_DPD_DEF, na.rm = TRUE),
    cc_SK_DPD_DEF_max  = safe_max(SK_DPD_DEF),
    cc_SK_DPD_DEF_sum  = safe_sum(SK_DPD_DEF, na.rm = TRUE),
    
    .groups = "drop"
  ) |>
  mutate(
    cc_SK_DPD_ever     = as.integer(cc_SK_DPD_max > 0),
    cc_SK_DPD_DEF_ever = as.integer(cc_SK_DPD_DEF_max > 0)
  )

# ============================================================================
# credit_card_balance: Aggregate credit-level features to client-level
#
# Each client may have multiple previous credit card accounts.
# This step summarizes across all accounts:
#   - Count of previous credit cards
#   - Mean / max / sum of balances, payments, drawings, and delinquency
#   - Ever delinquent rate → fraction of accounts ever past due
#
# Result: one row per client, ready to merge with other client-level tables
# ============================================================================
credit_card_client <- cc_prev_features |>
  group_by(SK_ID_CURR) |>
  summarise(
    COUNT_CREDIT_CARDS = n(),
    
    cc_AMT_BALANCE_mean     = safe_mean(cc_AMT_BALANCE_mean, na.rm = TRUE),
    cc_AMT_BALANCE_max      = safe_max(cc_AMT_BALANCE_max),
    cc_AMT_BALANCE_sum      = safe_sum(cc_AMT_BALANCE_sum, na.rm = TRUE),
    
    cc_AMT_CREDIT_LIMIT_ACTUAL_mean = safe_mean(cc_AMT_CREDIT_LIMIT_ACTUAL_mean, na.rm = TRUE),
    cc_AMT_CREDIT_LIMIT_ACTUAL_max  = safe_max(cc_AMT_CREDIT_LIMIT_ACTUAL_max),
    
    cc_AMT_PAYMENT_CURRENT_sum      = safe_sum(cc_AMT_PAYMENT_CURRENT_sum, na.rm = TRUE),
    cc_AMT_PAYMENT_TOTAL_CURRENT_sum = safe_sum(cc_AMT_PAYMENT_TOTAL_CURRENT_sum, na.rm = TRUE),
    
    cc_CNT_DRAWINGS_CURRENT_sum     = safe_sum(cc_CNT_DRAWINGS_CURRENT_sum, na.rm = TRUE),
    cc_CNT_INSTALMENT_MATURE_CUM    = safe_max(cc_CNT_INSTALMENT_MATURE_CUM),
    
    cc_SK_DPD_mean     = safe_mean(cc_SK_DPD_mean, na.rm = TRUE),
    cc_SK_DPD_max      = safe_max(cc_SK_DPD_max),
    cc_SK_DPD_sum      = safe_sum(cc_SK_DPD_sum, na.rm = TRUE),
    
    cc_SK_DPD_DEF_mean = safe_mean(cc_SK_DPD_DEF_mean, na.rm = TRUE),
    cc_SK_DPD_DEF_max  = safe_max(cc_SK_DPD_DEF_max),
    cc_SK_DPD_DEF_sum  = safe_sum(cc_SK_DPD_DEF_sum, na.rm = TRUE),
    
    cc_SK_DPD_ever_rate     = safe_mean(cc_SK_DPD_ever, na.rm = TRUE),
    cc_SK_DPD_DEF_ever_rate = safe_mean(cc_SK_DPD_DEF_ever, na.rm = TRUE),
    
    .groups = "drop"
  )

# ============================================================================
# previous_application: Aggregate previous application data to client-level
#
# Each client (SK_ID_CURR) may have multiple previous applications.
# This step summarizes across all applications to create client-level metrics:
#   - Count of previous applications
#   - Mean, max, min of requested/approved amounts, annuities
#   - Mean, max, min of days to decision, first/last due, termination
#   - Fraction approved / refused, last application flags
#
# Result: one row per client, ready to merge with other client-level tables
# ============================================================================
prev_app_client <- prev_app |>
  group_by(SK_ID_CURR) |>
  summarise(
    COUNT_PREV_APPLICATIONS = n(),
    
    # Amounts requested / approved
    prev_AMT_APPLICATION_mean = safe_mean(AMT_APPLICATION, na.rm = TRUE),
    prev_AMT_APPLICATION_max  = safe_max(AMT_APPLICATION),
    prev_AMT_CREDIT_mean      = safe_mean(AMT_CREDIT, na.rm = TRUE),
    prev_AMT_CREDIT_max       = safe_max(AMT_CREDIT),
    prev_AMT_ANNUITY_mean     = safe_mean(AMT_ANNUITY, na.rm = TRUE),
    prev_AMT_ANNUITY_max      = safe_max(AMT_ANNUITY),
    
    # Days relative to current application
    prev_DAYS_DECISION_mean     = safe_mean(DAYS_DECISION, na.rm = TRUE),
    prev_DAYS_FIRST_DRAWING_min = safe_min(DAYS_FIRST_DRAWING),
    prev_DAYS_LAST_DUE_max      = safe_max(DAYS_LAST_DUE),
    prev_DAYS_TERMINATION_max   = safe_max(DAYS_TERMINATION),
    
    # Flags
    prev_FLAG_LAST_APPL_PER_CONTRACT_rate =
      safe_mean(FLAG_LAST_APPL_PER_CONTRACT == "Y", na.rm = TRUE),

    prev_NFLAG_LAST_APPL_IN_DAY_rate =
      safe_mean(NFLAG_LAST_APPL_IN_DAY, na.rm = TRUE),
    
    # Application outcome
    prev_APPROVED_rate = safe_mean(NAME_CONTRACT_STATUS == "Approved", na.rm = TRUE),
    prev_REFUSED_rate  = safe_mean(NAME_CONTRACT_STATUS == "Refused", na.rm = TRUE),
    
    .groups = "drop"
  )

# ============================================================================
# installments_payments: Aggregate monthly installment data to loan-level
#
# Each previous loan (SK_ID_PREV) has multiple installments.
# This step summarizes each loan:
#   - Number of installments
#   - Mean / max / sum of scheduled and actual amounts
#   - Delay in payments (late payments)
#   - Ever late flag
#
# Result: one row per previous loan, ready for client-level aggregation
# ============================================================================
install_prev_features <- installments |>
  mutate(
    inst_delay = DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT
  ) |>
  group_by(SK_ID_CURR, SK_ID_PREV) |>
  summarise(
    inst_NUM_INSTALMENTS = n(),
    
    inst_AMT_INSTALMENT_mean = safe_mean(AMT_INSTALMENT, na.rm = TRUE),
    inst_AMT_INSTALMENT_sum  = safe_sum(AMT_INSTALMENT, na.rm = TRUE),
    
    inst_AMT_PAYMENT_mean = safe_mean(AMT_PAYMENT, na.rm = TRUE),
    inst_AMT_PAYMENT_sum  = safe_sum(AMT_PAYMENT, na.rm = TRUE),
    
    inst_delay_mean = safe_mean(inst_delay, na.rm = TRUE),
    inst_delay_max  = ifelse(
      all(is.na(inst_delay)),
      NA_real_,
      max(inst_delay, na.rm = TRUE)
    ),
    
    .groups = "drop"
  ) |>
  mutate(
    inst_ever_late = as.integer(inst_delay_max > 0)
  )

# ============================================================================
# installments_payments: Aggregate loan-level features to client-level
#
# Each client may have multiple previous loans.
# Summarize across loans:
#   - Total number of installments
#   - Mean / max / sum of scheduled and actual payments
#   - Average / max delay
#   - Fraction of loans ever late
#
# Result: one row per client, ready to merge with other client-level tables
# ============================================================================
install_client <- install_prev_features |>
  group_by(SK_ID_CURR) |>
  summarise(
    COUNT_INSTALLMENT_LOANS = n(),
    
    inst_AMT_INSTALMENT_mean = safe_mean(inst_AMT_INSTALMENT_mean, na.rm = TRUE),
    inst_AMT_INSTALMENT_sum  = safe_sum(inst_AMT_INSTALMENT_sum, na.rm = TRUE),
    
    inst_AMT_PAYMENT_mean = safe_mean(inst_AMT_PAYMENT_mean, na.rm = TRUE),
    inst_AMT_PAYMENT_sum  = safe_sum(inst_AMT_PAYMENT_sum, na.rm = TRUE),
    
    inst_delay_mean = safe_mean(inst_delay_mean, na.rm = TRUE),
    inst_delay_max = safe_max(inst_delay_max),
    
    inst_ever_late_rate = safe_mean(inst_ever_late, na.rm = TRUE),
    
    .groups = "drop"
  )

# =========================================
# Compute training-only statistics
# =========================================
compute_training_stats <- function(train) {
  list(
    EXT_SOURCE_1_median = median(train$EXT_SOURCE_1, na.rm = TRUE),
    EXT_SOURCE_2_median = median(train$EXT_SOURCE_2, na.rm = TRUE),
    EXT_SOURCE_3_median = median(train$EXT_SOURCE_3, na.rm = TRUE),
    CNT_FAM_MEMBERS_median = median(train$CNT_FAM_MEMBERS, na.rm = TRUE),
    AMT_ANNUITY_median = median(train$AMT_ANNUITY, na.rm = TRUE),
    AMT_GOODS_PRICE_median = median(train$AMT_GOODS_PRICE, na.rm = TRUE),

    # Age bins based on training data only
    AGE_bins = quantile(
      abs(train$DAYS_BIRTH) / 365,
      probs = c(0, 0.25, 0.5, 0.75, 1),
      na.rm = TRUE
    )
  )
}

# =========================================
# Identify building-level numeric vars (except categorical)
# =========================================
normalized_building_vars <- dict |>
  filter(
    Table == "application_{train|test}.csv",
    Description == "Normalized information about building where the client lives, What is average (_AVG suffix), modus (_MODE suffix), median (_MEDI suffix) apartment size, common area, living area, age of building, number of elevators, number of entrances, state of the building, number of floor"
  ) |>
  pull(Row) |>
  setdiff(c("FONDKAPREMONT_MODE", "HOUSETYPE_MODE", "WALLSMATERIAL_MODE", "EMERGENCYSTATE_MODE"))

# Function for obtaining the most frequent value (mode)
get_mode <- function(x) {
  ux <- na.omit(unique(x))
  ux[which.max(tabulate(match(x, ux)))]
}

# =========================================
# Update training stats to include building & categorical modes
# =========================================
compute_training_stats <- function(train) {
  list(
    EXT_SOURCE_1_median      = median(train$EXT_SOURCE_1, na.rm = TRUE),
    EXT_SOURCE_2_median      = median(train$EXT_SOURCE_2, na.rm = TRUE),
    EXT_SOURCE_3_median      = median(train$EXT_SOURCE_3, na.rm = TRUE),
    CNT_FAM_MEMBERS_median   = median(train$CNT_FAM_MEMBERS, na.rm = TRUE),
    AMT_ANNUITY_median       = median(train$AMT_ANNUITY, na.rm = TRUE),
    AMT_GOODS_PRICE_median   = median(train$AMT_GOODS_PRICE, na.rm = TRUE),

    # Modes for categorical variables
    NAME_TYPE_SUITE_mode     = get_mode(train$NAME_TYPE_SUITE),
    OCCUPATION_TYPE_mode     = get_mode(train$OCCUPATION_TYPE),
    FONDKAPREMONT_MODE_mode  = get_mode(train$FONDKAPREMONT_MODE),
    HOUSETYPE_MODE_mode      = get_mode(train$HOUSETYPE_MODE),
    WALLSMATERIAL_MODE_mode  = get_mode(train$WALLSMATERIAL_MODE),
    EMERGENCYSTATE_MODE_mode = get_mode(train$EMERGENCYSTATE_MODE),

    # Age bins
    AGE_bins = quantile(abs(train$DAYS_BIRTH) / 365,
                        probs = c(0, 0.25, 0.5, 0.75, 1),
                        na.rm = TRUE)
  )
}

# ========================================================
# Impute aggregated product-level features
# ========================================================
impute_client_aggregates <- function(df) {
  
  # Define groups of features
  count_vars <- c(
    "COUNT_POS_LOANS", "COUNT_BUREAU_RECORDS", "COUNT_CREDIT_CARDS",
    "COUNT_PREV_APPLICATIONS", "COUNT_INSTALLMENT_LOANS"
  )
  
  pos_vars <- grep("^pos_", names(df), value = TRUE)
  bureau_vars <- grep("^bureau_", names(df), value = TRUE)
  cc_vars <- grep("^cc_", names(df), value = TRUE)
  prev_vars <- grep("^prev_", names(df), value = TRUE)
  inst_vars <- grep("^inst_", names(df), value = TRUE)
  
  bureau_request_vars <- grep("^AMT_REQ_CREDIT_BUREAU_", names(df), value = TRUE)
  
  # All features to zero-fill
  zero_fill_vars <- c(
    count_vars,
    pos_vars,
    bureau_vars,
    cc_vars,
    prev_vars,
    inst_vars,
    bureau_request_vars
  )
  
  # Zero-fill NAs
  df <- df %>%
    mutate(across(all_of(zero_fill_vars), ~replace_na(.x, 0)))
  
  # Optionally, create "ever had" indicators if needed
  # For example: Did the client ever have a POS loan?
  df <- df %>%
    mutate(
      HAS_POS_LOAN = as.integer(COUNT_POS_LOANS > 0),
      HAS_BUREAU_RECORD = as.integer(COUNT_BUREAU_RECORDS > 0),
      HAS_CREDIT_CARD = as.integer(COUNT_CREDIT_CARDS > 0),
      HAS_PREV_APP = as.integer(COUNT_PREV_APPLICATIONS > 0),
      HAS_INSTALLMENT_LOAN = as.integer(COUNT_INSTALLMENT_LOANS > 0)
    )
  
  return(df)
}

# ==============================================================================
# Extend clean_application_data to handle building vars and categorical modes
# ==============================================================================
clean_application_data <- function(df, stats) {
  df |>
    mutate(
      # Missing indicators
      EXT_SOURCE_1_missing = as.integer(is.na(EXT_SOURCE_1)),
      EXT_SOURCE_2_missing = as.integer(is.na(EXT_SOURCE_2)),
      EXT_SOURCE_3_missing = as.integer(is.na(EXT_SOURCE_3)),

      # Impute numeric
      EXT_SOURCE_1      = replace_na(EXT_SOURCE_1, stats$EXT_SOURCE_1_median),
      EXT_SOURCE_2      = replace_na(EXT_SOURCE_2, stats$EXT_SOURCE_2_median),
      EXT_SOURCE_3      = replace_na(EXT_SOURCE_3, stats$EXT_SOURCE_3_median),
      CNT_FAM_MEMBERS   = replace_na(CNT_FAM_MEMBERS, stats$CNT_FAM_MEMBERS_median),
      AMT_ANNUITY       = replace_na(AMT_ANNUITY, stats$AMT_ANNUITY_median),
      AMT_GOODS_PRICE   = replace_na(AMT_GOODS_PRICE, stats$AMT_GOODS_PRICE_median),
      bureau_DEBT_RATIO = replace_na(bureau_DEBT_RATIO, 0),
      OWN_CAR_AGE = replace_na(OWN_CAR_AGE, 0),
      OBS_30_CNT_SOCIAL_CIRCLE = replace_na(OBS_30_CNT_SOCIAL_CIRCLE, 0),
      DEF_30_CNT_SOCIAL_CIRCLE = replace_na(DEF_30_CNT_SOCIAL_CIRCLE, 0),
      OBS_60_CNT_SOCIAL_CIRCLE = replace_na(OBS_60_CNT_SOCIAL_CIRCLE, 0),
      DEF_60_CNT_SOCIAL_CIRCLE = replace_na(DEF_60_CNT_SOCIAL_CIRCLE, 0),
      DAYS_LAST_PHONE_CHANGE = replace_na(DAYS_LAST_PHONE_CHANGE, median(DAYS_LAST_PHONE_CHANGE, na.rm = TRUE)),

      # Categorical imputation
      NAME_TYPE_SUITE   = replace_na(NAME_TYPE_SUITE, stats$NAME_TYPE_SUITE_mode),
      OCCUPATION_TYPE   = replace_na(OCCUPATION_TYPE, stats$OCCUPATION_TYPE_mode),
      FONDKAPREMONT_MODE= replace_na(FONDKAPREMONT_MODE, stats$FONDKAPREMONT_MODE_mode),
      HOUSETYPE_MODE    = replace_na(HOUSETYPE_MODE, stats$HOUSETYPE_MODE_mode),
      WALLSMATERIAL_MODE= replace_na(WALLSMATERIAL_MODE, stats$WALLSMATERIAL_MODE_mode),
      EMERGENCYSTATE_MODE_missing = as.integer(is.na(EMERGENCYSTATE_MODE)),
      EMERGENCYSTATE_MODE = replace_na(EMERGENCYSTATE_MODE, stats$EMERGENCYSTATE_MODE_mode),

      # Building-level numeric vars: zero-fill
      across(all_of(normalized_building_vars), ~replace_na(.x, 0)),

      # Fix demographic days
      DAYS_EMPLOYED     = ifelse(DAYS_EMPLOYED == 365243, 0, abs(DAYS_EMPLOYED)),
      AGE_YEARS         = abs(DAYS_BIRTH) / 365,

      # Financial ratios
      CREDIT_TO_INCOME  = AMT_CREDIT / (AMT_INCOME_TOTAL + 1),
      ANNUITY_TO_INCOME = AMT_ANNUITY / (AMT_INCOME_TOTAL + 1),
      LOAN_TO_GOODS     = AMT_CREDIT / (AMT_GOODS_PRICE + 1),
      CREDIT_PER_PERSON = AMT_CREDIT / (CNT_FAM_MEMBERS + 1),

      # Binning
      AGE_BIN = cut(AGE_YEARS,
                    breaks = stats$AGE_bins,
                    include.lowest = TRUE),
      AGE_BIN           = replace_na(AGE_BIN, get_mode(AGE_BIN))
    )
}

# =========================================
# Function to recode rare factor levels
# =========================================
recode_rare_factors <- function(df) {

  # CODE_GENDER
  df$CODE_GENDER <- as.factor(df$CODE_GENDER)
  if(!"F" %in% levels(df$CODE_GENDER)) {
    levels(df$CODE_GENDER) <- c(levels(df$CODE_GENDER), "F")
  }
  df$CODE_GENDER[df$CODE_GENDER == "XNA"] <- "F"

  # NAME_TYPE_SUITE
  df$NAME_TYPE_SUITE <- as.factor(df$NAME_TYPE_SUITE)
  if(!"Other" %in% levels(df$NAME_TYPE_SUITE)) {
    levels(df$NAME_TYPE_SUITE) <- c(levels(df$NAME_TYPE_SUITE), "Other")
  }
  rare_levels_suite <- c("Other_A", "Other_B", "Group of people")
  df$NAME_TYPE_SUITE[df$NAME_TYPE_SUITE %in% rare_levels_suite] <- "Other"
  df$NAME_TYPE_SUITE <- droplevels(df$NAME_TYPE_SUITE)

  # NAME_INCOME_TYPE
  df$NAME_INCOME_TYPE <- as.factor(df$NAME_INCOME_TYPE)
  if(!"Other" %in% levels(df$NAME_INCOME_TYPE)) {
    levels(df$NAME_INCOME_TYPE) <- c(levels(df$NAME_INCOME_TYPE), "Other")
  }
  rare_income_levels <- c("Businessman", "Maternity leave", "Student", "Unemployed")
  df$NAME_INCOME_TYPE[df$NAME_INCOME_TYPE %in% rare_income_levels] <- "Other"
  df$NAME_INCOME_TYPE <- droplevels(df$NAME_INCOME_TYPE)

  # NAME_EDUCATION_TYPE
  df$NAME_EDUCATION_TYPE <- as.factor(df$NAME_EDUCATION_TYPE)
  df <- df |>
    mutate(NAME_EDUCATION_TYPE = case_when(
      NAME_EDUCATION_TYPE %in% c("Lower secondary") ~ "Lower secondary",
      NAME_EDUCATION_TYPE %in% c("Secondary / secondary special") ~ "Secondary / secondary special",
      NAME_EDUCATION_TYPE %in% c("Incomplete higher") ~ "Incomplete higher",
      NAME_EDUCATION_TYPE %in% c("Higher education", "Academic degree") ~ "Academic / Bachelors or higher",
      TRUE ~ "Other"
    ))

  # NAME_FAMILY_STATUS
  df$NAME_FAMILY_STATUS <- as.factor(df$NAME_FAMILY_STATUS)
  if(!"Married" %in% levels(df$NAME_FAMILY_STATUS)) {
    levels(df$NAME_FAMILY_STATUS) <- c(levels(df$NAME_FAMILY_STATUS), "Married")
  }
  df <- df |>
    mutate(NAME_FAMILY_STATUS = ifelse(NAME_FAMILY_STATUS == "Unknown",
                                       "Married",
                                       as.character(NAME_FAMILY_STATUS))) |>
    mutate(NAME_FAMILY_STATUS = as.factor(NAME_FAMILY_STATUS))

  # NAME_HOUSING_TYPE
  df$NAME_HOUSING_TYPE <- as.factor(df$NAME_HOUSING_TYPE)
  df <- df |>
    mutate(NAME_HOUSING_TYPE = case_when(
      NAME_HOUSING_TYPE %in% c("Co-op apartment", "Office apartment") ~ "Other",
      NAME_HOUSING_TYPE %in% c("Municipal apartment", "Rented apartment") ~ "Rented / Municipal",
      TRUE ~ as.character(NAME_HOUSING_TYPE)
    )) |>
    mutate(NAME_HOUSING_TYPE = as.factor(NAME_HOUSING_TYPE))

  # OCCUPATION_TYPE
  df$OCCUPATION_TYPE <- as.factor(df$OCCUPATION_TYPE)
  df <- df |>
    mutate(OCCUPATION_TYPE = case_when(
      OCCUPATION_TYPE %in% c("Laborers", "Core staff", "Sales staff", "Managers", "Drivers") ~ as.character(OCCUPATION_TYPE),
      TRUE ~ "Other"
    )) |>
    mutate(OCCUPATION_TYPE = as.factor(OCCUPATION_TYPE))

  # ORGANIZATION_TYPE
  df$ORGANIZATION_TYPE <- as.factor(df$ORGANIZATION_TYPE)
  df <- df |>
    mutate(ORGANIZATION_TYPE = case_when(
      str_detect(ORGANIZATION_TYPE, "^Industry") ~ "Industry",
      str_detect(ORGANIZATION_TYPE, "^Trade") ~ "Trade",
      str_detect(ORGANIZATION_TYPE, "^Transport") ~ "Transport",
      ORGANIZATION_TYPE %in% c("Business Entity Type 3", "Government",
                               "Construction", "Housing", "Medicine",
                               "Self-employed", "School", "XNA") ~ as.character(ORGANIZATION_TYPE),
      TRUE ~ "Other"
    )) |>
    mutate(ORGANIZATION_TYPE = as.factor(ORGANIZATION_TYPE))

  # WALLSMATERIAL_MODE
  df$WALLSMATERIAL_MODE <- as.factor(df$WALLSMATERIAL_MODE)
  rare_walls <- c("Monolithic", "Mixed", "Others")
  df <- df |>
    mutate(WALLSMATERIAL_MODE = ifelse(WALLSMATERIAL_MODE %in% rare_walls,
                                       "Other",
                                       as.character(WALLSMATERIAL_MODE))) |>
    mutate(WALLSMATERIAL_MODE = as.factor(WALLSMATERIAL_MODE))

  return(df)
}

# ==========================
# Merge Client-Level Data
# ==========================
augment_application <- function(app_df) {
  app_df |>
    left_join(pos_cash_client, by = "SK_ID_CURR") |>
    left_join(bureau_client, by = "SK_ID_CURR") |>
    left_join(credit_card_client, by = "SK_ID_CURR") |>
    left_join(prev_app_client, by = "SK_ID_CURR") |>
    left_join(install_client, by = "SK_ID_CURR")
}

# Apply augmentation
train_aug <- augment_application(train)
test_aug  <- augment_application(test)

# Zero-fill and create indicators
train_aug <- impute_client_aggregates(train_aug)
test_aug  <- impute_client_aggregates(test_aug)

# Then apply your clean_application_data()
training_stats <- compute_training_stats(train_aug)
train_aug <- clean_application_data(train_aug, training_stats)
test_aug  <- clean_application_data(test_aug, training_stats)

# Address rare factor levels
train_aug <- recode_rare_factors(train_aug)
test_aug  <- recode_rare_factors(test_aug)


# =========================
# Write Final Data Sets
# =========================
write_csv(train_aug, file.path(data_dir, "train_final.csv"))
write_csv(test_aug,  file.path(data_dir, "test_final.csv"))