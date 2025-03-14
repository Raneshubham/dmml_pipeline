import pandas as pd
import logging
import os
import pickle
from tabulate import tabulate
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report

# Paths
data_path = r"C:\Users\ranes\Desktop\DMML\data\processed_data.csv"
log_path = r"C:\Users\ranes\Desktop\DMML\execution.log"
model_path = r"C:\Users\ranes\Desktop\DMML\models\trained_model.pkl"

# Setup logging
logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s - %(message)s')

def load_data(filepath):
    logging.info("Loading processed dataset.")
    df = pd.read_csv(filepath)
    logging.info(f"Dataset columns: {df.columns.tolist()}")
    return df

def train_random_forest(df):
    logging.info("Training Random Forest model.")
    
    # Ensure 'Status' column exists
    if 'Status' not in df.columns:
        logging.error("Error: 'Status' column not found in dataset.")
        raise KeyError("'Status' column not found in dataset.")
    
    X = df.drop(columns=['Status'])
    y = df['Status'].astype(int)  # Convert Status to integer
    
    # Splitting the data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Train model
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    # Save model
    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
    logging.info(f"Model saved at: {model_path}")
    
    # Predictions
    y_pred = model.predict(X_test)
    
    # Metrics
    accuracy = accuracy_score(y_test, y_pred)
    report_dict = classification_report(y_test, y_pred, output_dict=True)  # Get dict format
    
    # Convert classification report to DataFrame
    report_df = pd.DataFrame(report_dict).transpose()
    
    # Format report
    formatted_report = tabulate(report_df, headers="keys", tablefmt="pretty", floatfmt=".4f")
    
    logging.info(f"Model Accuracy: {accuracy:.4f}")
    logging.info(f"Classification Report:\n{formatted_report}")
    
    print(f"\nModel Accuracy: {accuracy:.4f}\n")
    print("Classification Report:")
    print(formatted_report)  # Print in a neat tabular format
    
    return model

if __name__ == "__main__":
    try:
        df = load_data(data_path)
        model = train_random_forest(df)
        logging.info("Random Forest model training completed successfully.")
        print(f"Trained model saved at: {model_path}")
    except Exception as e:
        logging.error(f"Error during execution: {e}")
        print(f"An error occurred: {e}")
