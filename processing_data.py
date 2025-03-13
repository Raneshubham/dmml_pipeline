import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import os
import logging
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.feature_selection import SelectKBest, f_classif

# Paths
data_path = r"C:\Users\ranes\Desktop\DMML\data\preprocessed_data.csv"
processed_data_path = r"C:\Users\ranes\Desktop\DMML\data\processed_data.csv"
log_path = r"C:\Users\ranes\Desktop\DMML\execution.log"
image_path = r"C:\Users\ranes\Desktop\DMML\images\heatmap.png"

# Setup logging
logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s - %(message)s')

def load_data(filepath):
    logging.info("Loading dataset.")
    return pd.read_csv(filepath)

def create_heatmap(df):
    logging.info("Creating heatmap.")
    df = df.copy()
    
    # Identify categorical columns
    categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
    if 'Status' in categorical_cols:
        categorical_cols.remove('Status')
    
    # Label encoding for categorical features before correlation matrix computation
    le = LabelEncoder()
    for col in categorical_cols:
        df[col] = le.fit_transform(df[col])
    
    plt.figure(figsize=(12, 8))
    sns.heatmap(df.corr(), annot=True, fmt='.2f', cmap='coolwarm')
    plt.title("Feature Correlation Heatmap")
    plt.savefig(image_path)
    plt.close()

def preprocess_data(df):
    logging.info("Preprocessing data.")
    df.dropna(inplace=True)  # Remove rows with missing values
    
    # Rename 'Active' column to 'Status'
    if 'Active' in df.columns:
        df.rename(columns={'Active': 'Status'}, inplace=True)
    
    # Ensure 'Status' column is retained
    if 'Status' not in df.columns:
        logging.error("'Status' column missing in input dataset. Aborting preprocessing.")
        raise ValueError("'Status' column missing in input dataset.")
    
    # Encode 'Status' column
    df['Status'] = df['Status'].map({'Active': 1, 'Inactive': 0})
    
    # Identify categorical columns
    categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
    if 'Status' in categorical_cols:
        categorical_cols.remove('Status')
    
    # Label encoding for categorical features except 'Status'
    le = LabelEncoder()
    for col in categorical_cols:
        df[col] = le.fit_transform(df[col])
    
    # Scaling numerical columns
    numerical_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
    scaler = StandardScaler()
    df[numerical_cols] = scaler.fit_transform(df[numerical_cols])
    
    # Remove highly correlated features
    correlation_matrix = df.corr().abs()
    upper_triangle = correlation_matrix.where(np.triu(np.ones(correlation_matrix.shape), k=1).astype(bool))
    high_corr_features = [column for column in upper_triangle.columns if any(upper_triangle[column] > 0.85)]
    df.drop(columns=high_corr_features, inplace=True)
    logging.info(f"Removed highly correlated features: {high_corr_features}")
    
    # Feature Selection using ANOVA F-test
    selected_features = df.columns.tolist()
    X = df.drop(columns=['Status'])
    y = df['Status']
    selector = SelectKBest(score_func=f_classif, k=min(10, X.shape[1]))
    X_new = selector.fit_transform(X, y)
    selected_features = X.columns[selector.get_support()].tolist()
    df = pd.DataFrame(X_new, columns=selected_features)
    df['Status'] = y
    logging.info(f"Selected top features: {selected_features}")
    
    logging.info("Feature selection completed.")
    return df

if __name__ == "__main__":
    df = load_data(data_path)
    create_heatmap(df)
    processed_df = preprocess_data(df)
    
    # Ensure 'Status' column is present before saving
    if 'Status' not in processed_df.columns:
        logging.error("Status column missing in processed dataset. Aborting saving.")
        raise ValueError("'Status' column missing in processed dataset.")
    
    processed_df.to_csv(processed_data_path, index=False)
    logging.info(f"Final dataframe columns: {processed_df.columns.tolist()}")
    print("Final dataframe columns:", processed_df.columns.tolist())