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

## Model Card Notebook

The **Credit Default Prediction Model Card** notebook provides a comprehensive, business-focused summary of the final LASSO model developed for predicting loan defaults. It is designed for stakeholders, analysts, and regulatory reviewers, highlighting model behavior, interpretability, and fairness.

Key content includes:

- **Business Context & Analytics Approach:** Explains the challenge of evaluating creditworthiness for applicants with limited credit history and the role of alternative data sources in estimating repayment probabilities.

- **Key Recommendations & Decision Thresholds:** Outlines how predicted probabilities can be used for loan approval, including a data-driven threshold that balances expected profit and default risk.

- **Model Details:** Documents the algorithm type, training procedure, hyperparameters, and training dataset composition, ensuring transparency and reproducibility.

- **Performance Metrics:** Reports cross-validated and hold-out validation AUC, threshold-dependent metrics (precision, recall, approval rate), expected profit analysis, and benchmark comparisons.

- **Interpretability & Feature Importance:** Highlights the top predictive features and their business interpretation, including a SHAP-like perspective using LASSO coefficients and visualizations.

- **Adverse Action & Fairness Analysis:** Maps features to human-readable denial reasons for compliance and includes approval rate analysis across demographic groups to assess and mitigate potential bias.

- **Deployment Considerations & Caveats:** Discusses limitations, regulatory constraints, and considerations for applying the model to new populations or products.

This notebook complements the modeling workflow by translating technical outputs into actionable insights for decision-making, lending transparency, and regulatory compliance.

## Repository Structure

The repository contains the following files and folders:

## Repository Structure

- `notebooks/`  
  Contains all Quarto source documents (`.qmd`) for the project, including:
  - `EDA.qmd` – Exploratory data analysis of the raw CSV datasets.
  - `Modeling.qmd` – Model experiments, evaluation metrics, and final model selection.
  - `Model_Card.qmd` – Final model summary, threshold analysis, feature importance, and fairness considerations.

- `*.html`  
  Rendered HTML outputs of the Quarto notebooks, located in the root of the repository for easy viewing.

- `BusinessProblemStatement.pdf`  
  Formal description of the business objective, stakeholders, constraints, and success criteria.

- `Feature_Engineering.R`  
  R script for data cleaning and feature engineering.

- `README.md`  
  Project documentation and overview.
