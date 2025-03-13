from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
import subprocess
import logging
from prefect.schedules import CronSchedule

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define tasks
@task
def run_load_to_sqlite():
    subprocess.run(["python", "load_to_sqlite.py"], check=True)

@task
def run_load_and_join():
    subprocess.run(["python", "load_and_join.py"], check=True)

@task
def run_data_preprocessing():
    subprocess.run(["python", "data_preprocessing.py"], check=True)

@task
def run_processing_data():
    subprocess.run(["python", "processing_data.py"], check=True)

@task
def run_train_model():
    subprocess.run(["python", "train_model.py"], check=True)

@flow(task_runner=SequentialTaskRunner(), name="data_pipeline")
def data_pipeline():
    run_load_to_sqlite()
    run_load_and_join()
    run_data_preprocessing()
    run_processing_data()
    run_train_model()

if __name__ == "__main__":
    from prefect.server.api.schedules import create_schedule

    # Schedule to run every hour
    schedule = create_schedule(cron="0 * * * *", flow_name="data_pipeline")
    data_pipeline()
