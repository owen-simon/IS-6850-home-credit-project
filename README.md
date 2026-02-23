# IS-6850 Home Credit Project

This repository contains my University of Utah MSBA Capstone Project for IS 6850. The project focuses on feature engineering and data preparation for the **Home Credit Default Risk** dataset, producing a high-quality dataset suitable for predictive modeling.

## Project Overview

The goal of this project is to clean, transform, and enrich application data to improve model performance and reliability. Key tasks include:

- **Data Cleaning**
  - Fix anomalies such as `DAYS_EMPLOYED = 365243`  
  - Convert negative day values (age, employment duration) to positive  
  - Impute missing values for important features like `EXT_SOURCE_`  
  - Generate missing-value indicators

- **Feature Engineering**
  - Update demographic features: age, employment duration, family size  
  - Create financial ratios: credit-to-income, annuity-to-income, loan-to-goods price, credit per person  
  - Generate binned and interaction features from EDA insights  

- **Aggregating Supplementary Data**
  - Aggregate `POS_CASH_balance`, `bureau`, `bureau_balance`, `credit_card_balance`, `previous_application`, and `installments_payments` to client-level metrics  
  - Compute counts, sums, averages, delinquency flags, and ever-overdue indicators  

- **Ensuring Train/Test Consistency**
  - Medians, modes, and bin thresholds are computed from training data only  
  - Aggregated features are zero-filled and consistent across train and test sets  

- **Reusable Functions**
  - Modular R functions allow repeated application to both train and test datasets  
  - Functions include cleaning, aggregating, imputing, and engineering features  

## Modeling Summary

The modeling notebook demonstrates the predictive modeling workflow applied to the processed dataset:

- Models Explored
  - Logistic Regression
  - LASSO
  - Random Forest
- Performance Evaluation
  - Models were evaluated using cross-validated ROC AUC
  - LASSO achieved the highest CV AUC scores while maintaining generalizability on the test set
  - Logistic regression achieved similar performance but used significantly more predictors
- Final Model Selection
  - The caret package LASSO model was selected as the final model for submission due to its balance of interpretability, consistent performance, and ability to quickly tune hyperparameters
  - Test set predictions and CV metrics are available in the notebook
- Reusable Modeling Workflow
  - Code is structured to allow re-fitting and evaluation on new datasets
  - CV and test metrics are clearly labeled for easy comparison

## Repository Structure

The repository contains the following files and folders:

- `data-files/`           Contains raw and processed CSV datasets:
  - `train_final.csv`     The processed training dataset.
  - `test_final.csv`      The processed test dataset.
- `EDA.qmd`               Exploratory data analysis of the raw CSV datasets.
- `Feature_Engineering.R` R script for data cleaning and feature engineering.
- `Modeling.qmd`     Notebook documenting model trails, evaluation metrics, and final model selection.
- `README.md`             This project documentation.
