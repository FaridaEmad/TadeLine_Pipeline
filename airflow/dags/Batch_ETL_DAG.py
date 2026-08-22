from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# ============================================================
# Configuration
# ============================================================

SSH_KEY = "/home/airflow/.ssh/itvdelab.key"
SSH_USER = "itversity"
SSH_HOST = "itvdelab"

SSH_OPTIONS = (
    f"-i {SSH_KEY} "
    "-o IdentitiesOnly=yes "
    "-o StrictHostKeyChecking=no "
    "-o UserKnownHostsFile=/dev/null"
)


def ssh_command(command):
    return (
        f"ssh {SSH_OPTIONS} "
        f"{SSH_USER}@{SSH_HOST} "
        f"'export HADOOP_HOME=/opt/hadoop && "
        f"export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop && "
        f"export HIVE_HOME=/opt/hive && "
        f"export PATH=$HADOOP_HOME/bin:"
        f"$HADOOP_HOME/sbin:"
        f"$HIVE_HOME/bin:"
        f"$PATH && "
        f"{command}'"
    )


# ============================================================
# Default Arguments
# ============================================================

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="batch_etl_pipeline",
    default_args=default_args,
    description="End-to-end Ports ETL: SFTP -> Bronze -> Silver -> Gold",
    start_date=datetime(2026, 8, 1),
    schedule_interval="@monthly",
    catchup=False,
    max_active_runs=1,
    tags=["ports", "etl", "bronze", "silver", "gold"],
) as dag:

    # ========================================================
    # 1. Bronze Layer
    # ========================================================

    bronze_ports = BashOperator(
        task_id="bronze_ports_ingestion",
        bash_command=ssh_command(
            "bash /bronze_scripts/load_ports_to_hdfs.sh"
        ),
    )


    # ========================================================
    # 2. Silver Layer
    # ========================================================

    silver_ports = BashOperator(
        task_id="silver_ports_etl",
        bash_command=ssh_command(
            "spark-submit "
            "/silver_scripts/Spark_job/ports_to_silver.py"
        ),
    )


    # ========================================================
    # 3. Repair Silver Hive Partitions
    # ========================================================

    repair_silver = BashOperator(
        task_id="repair_silver_partitions",
        bash_command=ssh_command(
            "hive -e "
            "\"MSCK REPAIR TABLE silver.ports; "
            "SELECT COUNT(*) FROM silver.ports;\""
        ),
    )


    # ========================================================
    # 4. Gold Layer
    # ========================================================

    gold_ports = BashOperator(
        task_id="gold_ports_etl",
        bash_command=ssh_command(
            "spark-submit "
            "/gold_scripts/Spark_job/Ports_to_gold.py"
        ),
    )


    # ========================================================
    # 5. Gold Validation
    # ========================================================

    validate_gold = BashOperator(
        task_id="validate_gold",
        bash_command=ssh_command(
            "hive -e "
            "\"SELECT COUNT(*) FROM gold.ports;\""
        ),
    )


    # ========================================================
    # Dependencies
    # ========================================================

    (
        bronze_ports
        >> silver_ports
        >> repair_silver
        >> gold_ports
        >> validate_gold
    )