import logging

# Configure logging
log_file = r"execution.log"
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(message)s')

def log(message):
    logging.info(message)
    print(message)


import subprocess

scripts = ["load_to_sqlite.py", "load_and_join.py", "partitioning.py", "data_exploration_and_visualization.py", "data_preprocessing.py", "processing_data.py", "create_feature_store_and_train_model.py"]

for script in scripts:
    print(f"Running {script}...")
    logging.info(f"Running {script}...")
    subprocess.run(["python", script])  # Runs each script
