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
safe_max <- function(x) if (all(is.na(x))) NA_real_ else max(x, na.rm = TRUE)
safe_min <- function(x) if (all(is.na(x))) NA_real_ else min(x, na.rm = TRUE)

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
    
    pos_CNT_INSTALMENT_mean = mean(CNT_INSTALMENT, na.rm = TRUE),
    pos_CNT_INSTALMENT_max  = safe_max(CNT_INSTALMENT),
    
    pos_CNT_INSTALMENT_FUTURE_mean = mean(CNT_INSTALMENT_FUTURE, na.rm = TRUE),
    pos_CNT_INSTALMENT_FUTURE_min  = safe_min(CNT_INSTALMENT_FUTURE),
    
    pos_SK_DPD_mean = mean(SK_DPD, na.rm = TRUE),
    pos_SK_DPD_max  = safe_max(SK_DPD),
    pos_SK_DPD_sum  = sum(SK_DPD, na.rm = TRUE),
    
    pos_SK_DPD_DEF_mean = mean(SK_DPD_DEF, na.rm = TRUE),
    pos_SK_DPD_DEF_max  = safe_max(SK_DPD_DEF),
    pos_SK_DPD_DEF_sum  = sum(SK_DPD_DEF, na.rm = TRUE),
    
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
    
    pos_CNT_INSTALMENT_mean = mean(pos_CNT_INSTALMENT_mean, na.rm = TRUE),
    pos_CNT_INSTALMENT_max  = safe_max(pos_CNT_INSTALMENT_max),
    
    pos_CNT_INSTALMENT_FUTURE_mean = mean(pos_CNT_INSTALMENT_FUTURE_mean, na.rm = TRUE),
    pos_CNT_INSTALMENT_FUTURE_min  = safe_min(pos_CNT_INSTALMENT_FUTURE_min),
    
    pos_SK_DPD_mean = mean(pos_SK_DPD_mean, na.rm = TRUE),
    pos_SK_DPD_max  = safe_max(pos_SK_DPD_max),
    pos_SK_DPD_sum  = sum(pos_SK_DPD_sum, na.rm = TRUE),
    
    pos_SK_DPD_DEF_mean = mean(pos_SK_DPD_DEF_mean, na.rm = TRUE),
    pos_SK_DPD_DEF_max  = safe_max(pos_SK_DPD_DEF_max),
    pos_SK_DPD_DEF_sum  = sum(pos_SK_DPD_DEF_sum, na.rm = TRUE),
    
    pos_SK_DPD_ever_rate     = mean(pos_SK_DPD_ever, na.rm = TRUE),
    pos_SK_DPD_DEF_ever_rate = mean(pos_SK_DPD_DEF_ever, na.rm = TRUE),
    
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
    
    bureau_AMT_CREDIT_SUM_mean  = mean(AMT_CREDIT_SUM, na.rm = TRUE),
    bureau_AMT_CREDIT_SUM_max   = safe_max(AMT_CREDIT_SUM),
    bureau_AMT_CREDIT_SUM_debt  = sum(AMT_CREDIT_SUM_DEBT, na.rm = TRUE),
    
    bureau_days_credit_mean     = mean(DAYS_CREDIT, na.rm = TRUE),
    bureau_days_credit_end_max  = safe_max(DAYS_CREDIT_ENDDATE),
    
    bureau_ever_overdue_rate = ifelse(all(is.na(bb_ever_overdue)), NA_real_,
         mean(bb_ever_overdue, na.rm = TRUE)),
    
    .groups = "drop"
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
    
    cc_AMT_BALANCE_mean     = mean(AMT_BALANCE, na.rm = TRUE),
    cc_AMT_BALANCE_max      = safe_max(AMT_BALANCE),
    cc_AMT_BALANCE_sum      = sum(AMT_BALANCE, na.rm = TRUE),
    
    cc_AMT_CREDIT_LIMIT_ACTUAL_mean = mean(AMT_CREDIT_LIMIT_ACTUAL, na.rm = TRUE),
    cc_AMT_CREDIT_LIMIT_ACTUAL_max  = safe_max(AMT_CREDIT_LIMIT_ACTUAL),
    
    cc_AMT_PAYMENT_CURRENT_sum  = sum(AMT_PAYMENT_CURRENT, na.rm = TRUE),
    cc_AMT_PAYMENT_TOTAL_CURRENT_sum = sum(AMT_PAYMENT_TOTAL_CURRENT, na.rm = TRUE),
    
    cc_CNT_DRAWINGS_CURRENT_sum = sum(CNT_DRAWINGS_CURRENT, na.rm = TRUE),
    cc_CNT_INSTALMENT_MATURE_CUM = safe_max(CNT_INSTALMENT_MATURE_CUM),
    
    cc_SK_DPD_mean     = mean(SK_DPD, na.rm = TRUE),
    cc_SK_DPD_max      = safe_max(SK_DPD),
    cc_SK_DPD_sum      = sum(SK_DPD, na.rm = TRUE),
    
    cc_SK_DPD_DEF_mean = mean(SK_DPD_DEF, na.rm = TRUE),
    cc_SK_DPD_DEF_max  = safe_max(SK_DPD_DEF),
    cc_SK_DPD_DEF_sum  = sum(SK_DPD_DEF, na.rm = TRUE),
    
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
    
    cc_AMT_BALANCE_mean     = mean(cc_AMT_BALANCE_mean, na.rm = TRUE),
    cc_AMT_BALANCE_max      = safe_max(cc_AMT_BALANCE_max),
    cc_AMT_BALANCE_sum      = sum(cc_AMT_BALANCE_sum, na.rm = TRUE),
    
    cc_AMT_CREDIT_LIMIT_ACTUAL_mean = mean(cc_AMT_CREDIT_LIMIT_ACTUAL_mean, na.rm = TRUE),
    cc_AMT_CREDIT_LIMIT_ACTUAL_max  = safe_max(cc_AMT_CREDIT_LIMIT_ACTUAL_max),
    
    cc_AMT_PAYMENT_CURRENT_sum      = sum(cc_AMT_PAYMENT_CURRENT_sum, na.rm = TRUE),
    cc_AMT_PAYMENT_TOTAL_CURRENT_sum = sum(cc_AMT_PAYMENT_TOTAL_CURRENT_sum, na.rm = TRUE),
    
    cc_CNT_DRAWINGS_CURRENT_sum     = sum(cc_CNT_DRAWINGS_CURRENT_sum, na.rm = TRUE),
    cc_CNT_INSTALMENT_MATURE_CUM    = safe_max(cc_CNT_INSTALMENT_MATURE_CUM),
    
    cc_SK_DPD_mean     = mean(cc_SK_DPD_mean, na.rm = TRUE),
    cc_SK_DPD_max      = safe_max(cc_SK_DPD_max),
    cc_SK_DPD_sum      = sum(cc_SK_DPD_sum, na.rm = TRUE),
    
    cc_SK_DPD_DEF_mean = mean(cc_SK_DPD_DEF_mean, na.rm = TRUE),
    cc_SK_DPD_DEF_max  = safe_max(cc_SK_DPD_DEF_max),
    cc_SK_DPD_DEF_sum  = sum(cc_SK_DPD_DEF_sum, na.rm = TRUE),
    
    cc_SK_DPD_ever_rate     = mean(cc_SK_DPD_ever, na.rm = TRUE),
    cc_SK_DPD_DEF_ever_rate = mean(cc_SK_DPD_DEF_ever, na.rm = TRUE),
    
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
    prev_AMT_APPLICATION_mean = mean(AMT_APPLICATION, na.rm = TRUE),
    prev_AMT_APPLICATION_max  = safe_max(AMT_APPLICATION),
    prev_AMT_CREDIT_mean      = mean(AMT_CREDIT, na.rm = TRUE),
    prev_AMT_CREDIT_max       = safe_max(AMT_CREDIT),
    prev_AMT_ANNUITY_mean     = mean(AMT_ANNUITY, na.rm = TRUE),
    prev_AMT_ANNUITY_max      = safe_max(AMT_ANNUITY),
    
    # Days relative to current application
    prev_DAYS_DECISION_mean     = mean(DAYS_DECISION, na.rm = TRUE),
    prev_DAYS_FIRST_DRAWING_min = safe_min(DAYS_FIRST_DRAWING),
    prev_DAYS_LAST_DUE_max      = safe_max(DAYS_LAST_DUE),
    prev_DAYS_TERMINATION_max   = safe_max(DAYS_TERMINATION),
    
    # Flags
    prev_FLAG_LAST_APPL_PER_CONTRACT_rate =
      mean(FLAG_LAST_APPL_PER_CONTRACT == "Y", na.rm = TRUE)

,
    prev_NFLAG_LAST_APPL_IN_DAY_rate = mean(NFLAG_LAST_APPL_IN_DAY, na.rm = TRUE),
    
    # Application outcome
    prev_APPROVED_rate = mean(NAME_CONTRACT_STATUS == "Approved", na.rm = TRUE),
    prev_REFUSED_rate  = mean(NAME_CONTRACT_STATUS == "Refused", na.rm = TRUE),
    
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
    
    inst_AMT_INSTALMENT_mean = mean(AMT_INSTALMENT, na.rm = TRUE),
    inst_AMT_INSTALMENT_sum  = sum(AMT_INSTALMENT, na.rm = TRUE),
    
    inst_AMT_PAYMENT_mean = mean(AMT_PAYMENT, na.rm = TRUE),
    inst_AMT_PAYMENT_sum  = sum(AMT_PAYMENT, na.rm = TRUE),
    
    inst_delay_mean = mean(inst_delay, na.rm = TRUE),
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
    
    inst_AMT_INSTALMENT_mean = mean(inst_AMT_INSTALMENT_mean, na.rm = TRUE),
    inst_AMT_INSTALMENT_sum  = sum(inst_AMT_INSTALMENT_sum, na.rm = TRUE),
    
    inst_AMT_PAYMENT_mean = mean(inst_AMT_PAYMENT_mean, na.rm = TRUE),
    inst_AMT_PAYMENT_sum  = sum(inst_AMT_PAYMENT_sum, na.rm = TRUE),
    
    inst_delay_mean = mean(inst_delay_mean, na.rm = TRUE),
    inst_delay_max = safe_max(inst_delay_max),
    
    inst_ever_late_rate = mean(inst_ever_late, na.rm = TRUE),
    
    .groups = "drop"
  )

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

# Apply augmentations
train_aug <- augment_application(train)
test_aug  <- augment_application(test)

# ===================
# Data Manipulation
# ===================

# Combine Training and Test Data Sets
test_aug <- test_aug |> 
  mutate(TARGET = NA)

combined <- bind_rows(
  train_aug |> mutate(dataset = "train"),
  test_aug  |> mutate(dataset = "test")
)

# Identify normalized building-level variables except the 4 categorical exceptions
normalized_building_vars <- dict |>
  filter(Table == "application_{train|test}.csv",
         Description == "Normalized information about building where the client lives, What is average (_AVG suffix), modus (_MODE suffix), median (_MEDI suffix) apartment size, common area, living area, age of building, number of elevators, number of entrances, state of the building, number of floor") |>
  pull(Row) |>
  setdiff(c("FONDKAPREMONT_MODE", "HOUSETYPE_MODE", "WALLSMATERIAL_MODE", "EMERGENCYSTATE_MODE"))

# Function for obtaining the most often response (mode) for categorical variables
get_mode <- function(x) {
  ux <- na.omit(unique(x))
  ux[which.max(tabulate(match(x, ux)))]
}

combined <- combined |>
  mutate(
  # Address Null Values
    AMT_ANNUITY = replace_na(AMT_ANNUITY, median(AMT_ANNUITY, na.rm = TRUE)),
    AMT_GOODS_PRICE = replace_na(AMT_GOODS_PRICE, median(AMT_GOODS_PRICE, na.rm = TRUE)),
    NAME_TYPE_SUITE = replace_na(NAME_TYPE_SUITE, "Unknown"),
    OWN_CAR_AGE = replace_na(OWN_CAR_AGE, 0),
    OCCUPATION_TYPE = replace_na(OCCUPATION_TYPE, "Unknown"),
    CNT_FAM_MEMBERS = replace_na(CNT_FAM_MEMBERS, median(CNT_FAM_MEMBERS, na.rm = TRUE)),
    EXT_SOURCE_1_missing = as.integer(is.na(EXT_SOURCE_1)),
    EXT_SOURCE_1 = replace_na(EXT_SOURCE_1, median(EXT_SOURCE_1, na.rm = TRUE)),
    EXT_SOURCE_2 = replace_na(EXT_SOURCE_2, median(EXT_SOURCE_2, na.rm = TRUE)),
    EXT_SOURCE_3_missing = as.integer(is.na(EXT_SOURCE_3)),
    EXT_SOURCE_3 = replace_na(EXT_SOURCE_3, median(EXT_SOURCE_3, na.rm = TRUE)),
    across(all_of(normalized_building_vars), ~replace_na(.x, 0)),
    FONDKAPREMONT_MODE = replace_na(FONDKAPREMONT_MODE, "None"),
    HOUSETYPE_MODE     = replace_na(HOUSETYPE_MODE, "Other"),
    WALLSMATERIAL_MODE = replace_na(WALLSMATERIAL_MODE, "Unknown"),
    EMERGENCYSTATE_MODE = replace_na(EMERGENCYSTATE_MODE, get_mode(EMERGENCYSTATE_MODE)),
    EMERGENCYSTATE_MODE_missing = as.integer(is.na(EMERGENCYSTATE_MODE)),
    OBS_30_CNT_SOCIAL_CIRCLE = replace_na(OBS_30_CNT_SOCIAL_CIRCLE, 0),
    DEF_30_CNT_SOCIAL_CIRCLE = replace_na(DEF_30_CNT_SOCIAL_CIRCLE, 0),
    OBS_60_CNT_SOCIAL_CIRCLE = replace_na(OBS_60_CNT_SOCIAL_CIRCLE, 0),
    DEF_60_CNT_SOCIAL_CIRCLE = replace_na(DEF_60_CNT_SOCIAL_CIRCLE, 0),
    DAYS_LAST_PHONE_CHANGE = replace_na(DAYS_LAST_PHONE_CHANGE, 0),
    across(starts_with("AMT_REQ_CREDIT_BUREAU_"), ~replace_na(.x, 0)),
    COUNT_POS_LOANS = replace_na(COUNT_POS_LOANS, 0),
    across(starts_with("pos_"), ~replace_na(.x, 0)),
    COUNT_BUREAU_RECORDS = replace_na(COUNT_BUREAU_RECORDS, 0),
    bureau_AMT_CREDIT_SUM_mean = replace_na(bureau_AMT_CREDIT_SUM_mean, 0),
    bureau_AMT_CREDIT_SUM_max = replace_na(bureau_AMT_CREDIT_SUM_max, 0),
    bureau_AMT_CREDIT_SUM_debt = replace_na(bureau_AMT_CREDIT_SUM_debt, 0),
    bureau_days_credit_mean = replace_na(bureau_days_credit_mean, 0),
    bureau_days_credit_end_max = replace_na(bureau_days_credit_end_max, 0),
    bureau_ever_overdue_missing = as.integer(is.na(bureau_ever_overdue_rate)),
    bureau_ever_overdue_rate = replace_na(bureau_ever_overdue_rate, 0),
    COUNT_CREDIT_CARDS = replace_na(COUNT_CREDIT_CARDS, 0),
    across(starts_with("cc_"), ~replace_na(.x, 0)),
    COUNT_PREV_APPLICATIONS = replace_na(COUNT_PREV_APPLICATIONS, 0),
    across(starts_with("prev_"), ~replace_na(.x, 0)),
    COUNT_INSTALLMENT_LOANS = replace_na(COUNT_INSTALLMENT_LOANS, 0),
    across(starts_with("inst_"), ~replace_na(.x, 0)),
  # `Days_` Variables no longer relative to the application
    DAYS_EMPLOYED = ifelse(DAYS_EMPLOYED == 0, 0, abs(DAYS_EMPLOYED)),
    AGE = abs(DAYS_BIRTH) / 365,
    DAYS_REGISTRATION = abs(DAYS_REGISTRATION),
    DAYS_ID_PUBLISH = abs(DAYS_ID_PUBLISH))

# ===================================
# Split Training and Test Data Sets
# ===================================
train_aug <- combined |> 
  filter(dataset == "train") |> 
  select(-dataset)

test_aug <- combined |> 
  filter(dataset == "test") |> 
  select(-dataset, -TARGET)

# =========================
# Write Final Data Sets
# =========================

write_csv(train_aug, file.path(data_dir, "train_final.csv"))
write_csv(test_aug,  file.path(data_dir, "test_final.csv"))