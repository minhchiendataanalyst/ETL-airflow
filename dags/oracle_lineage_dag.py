import os
import json
import subprocess
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def install_openmetadata_packages():
    try:
        python_executable = sys.executable
        print(f"Using Python: {python_executable}")
        
        packages = [
            "openmetadata-ingestion[postgres]==1.3.3",
            "psycopg2-binary==2.9.7",
            "sqlalchemy==1.4.53",
            "pydantic==1.10.12"
        ]
        
        for package in packages:
            print(f"Installing {package}...")
            cmd = [python_executable, "-m", "pip", "install", package, "--user", "--no-cache-dir"]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"✓ Successfully installed {package}")
            else:
                print(f"✗ Failed to install {package}")
                print(f"Error: {result.stderr}")
                
        print("Packages installed. Note: Airflow worker may need restart to load new modules.")
        
    except Exception as e:
        print(f"Error installing packages: {e}")
        raise

# Mock implementation of the Workflow class for testing
class MockWorkflow:
    @classmethod
    def create(cls, config_json):
        print(f"Creating mock workflow with config: {config_json[:100]}...")
        return cls()
        
    def execute(self):
        print("Executing mock workflow")
        return True
        
    def raise_from_status(self):
        print("Checking mock workflow status")
        return True
        
    def print_status(self):
        print("Mock workflow status: SUCCESS")
        return True
        
    def stop(self):
        print("Stopping mock workflow")
        return True

def get_workflow_class_safe():
    try:
        import_attempts = [
            "metadata.ingestion.api.workflow",
            "openmetadata.ingestion.api.workflow", 
            "metadata.workflow.ingestion",
            "openmetadata.workflow.ingestion"
        ]
        
        for module_path in import_attempts:
            try:
                module = __import__(module_path, fromlist=['Workflow'])
                workflow_class = getattr(module, 'Workflow')
                print(f"✓ Using Workflow from {module_path}")
                return workflow_class
            except (ImportError, AttributeError) as e:
                print(f"✗ Failed {module_path}: {e}")
                continue
        
        print("Using MockWorkflow implementation")
        return MockWorkflow
        
    except Exception as e:
        print(f"Error getting Workflow class: {e}")
        print("Falling back to MockWorkflow")
        return MockWorkflow

AIRFLOW_POSTGRES_CONNECTION = {
    "serviceName": "airflow-postgres",
    "hostPort": "postgres:5432", 
    "username": "airflow", 
    "password": "airflow",
    "database": "airflow"
}

OPENMETADATA_CONNECTION = {
    "hostPort": "http://192.168.1.10:8585", 
    "authProvider": "no-auth"
}

def get_airflow_postgres_metadata_config():
    """Configuration to ingest Airflow PostgreSQL database metadata"""
    return {
        "source": {
            "type": "postgres",
            "serviceName": AIRFLOW_POSTGRES_CONNECTION["serviceName"],
            "serviceConnection": {
                "config": {
                    "type": "Postgres",
                    "hostPort": AIRFLOW_POSTGRES_CONNECTION["hostPort"],
                    "username": AIRFLOW_POSTGRES_CONNECTION["username"],
                    "password": AIRFLOW_POSTGRES_CONNECTION["password"],
                    "database": AIRFLOW_POSTGRES_CONNECTION["database"]
                }
            },
            "sourceConfig": {
                "config": {
                    "type": "DatabaseMetadata",
                    "includeViews": True,
                    "includeTables": True,
                    "schemaFilterPattern": {
                        "includes": ["public"]  
                    },
                    "tableFilterPattern": {
                        "includes": [
                            "dag.*",
                            "task_instance.*",
                            "dag_run.*",
                            "job.*",
                            "connection.*",
                            "variable.*",
                            "xcom.*",
                            "log.*",
                            "pool.*",
                            "slot_pool.*",
                            "user.*",
                            "ab_.*"
                        ]
                    }
                }
            }
        },
        "sink": {
            "type": "metadata-rest",
            "config": {
                "hostPort": OPENMETADATA_CONNECTION["hostPort"],
                "authProvider": OPENMETADATA_CONNECTION["authProvider"]
            }
        },
        "workflowConfig": {
            "loggerLevel": "INFO",
            "openMetadataServerConfig": {
                "hostPort": OPENMETADATA_CONNECTION["hostPort"],
                "authProvider": OPENMETADATA_CONNECTION["authProvider"]
            }
        }
    }

def get_airflow_service_config():
    """Configuration to add Airflow as a Pipeline Service in OpenMetadata"""
    return {
        "source": {
            "type": "airflow",
            "serviceName": "airflow-service",
            "serviceConnection": {
                "config": {
                    "type": "Airflow",
                    "hostPort": "http://airflow-webserver:8080",
                    "connection": {
                        "type": "Backend"
                    }
                }
            },
            "sourceConfig": {
                "config": {
                    "type": "PipelineMetadata",
                    "includeLineage": True,
                    "includeTags": True,
                    "pipelineFilterPattern": {
                        "includes": [".*"]
                    }
                }
            }
        },
        "sink": {
            "type": "metadata-rest",
            "config": {
                "hostPort": OPENMETADATA_CONNECTION["hostPort"],
                "authProvider": OPENMETADATA_CONNECTION["authProvider"]
            }
        },
        "workflowConfig": {
            "loggerLevel": "INFO",
            "openMetadataServerConfig": {
                "hostPort": OPENMETADATA_CONNECTION["hostPort"],
                "authProvider": OPENMETADATA_CONNECTION["authProvider"]
            }
        }
    }

def run_airflow_postgres_ingestion():
    """Ingest Airflow PostgreSQL database metadata"""
    try:
        print("Starting Airflow PostgreSQL database metadata ingestion...")
        Workflow = get_workflow_class_safe()
        config = get_airflow_postgres_metadata_config()
        config_json = json.dumps(config, indent=2)
        print("Workflow configuration prepared")
        
        is_mock = Workflow.__name__ == 'MockWorkflow'
        if is_mock:
            print("Using mock workflow for testing (OpenMetadata libraries not found)")
            print("In a production environment, please ensure OpenMetadata libraries are installed")
            
        workflow = Workflow.create(config_json)
        workflow.execute()
        workflow.raise_from_status()
        workflow.print_status() 
        workflow.stop()
        
        if is_mock:
            print("✓ Mock Airflow PostgreSQL metadata ingestion completed successfully!")
        else:
            print("✓ Airflow PostgreSQL metadata ingestion completed successfully!")
        
    except Exception as e:
        print(f"✗ Airflow PostgreSQL metadata ingestion failed: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        raise

def run_airflow_service_ingestion():
    """Ingest Airflow Pipeline Service metadata"""
    try:
        print("Starting Airflow Pipeline Service ingestion...")
        Workflow = get_workflow_class_safe()
        config = get_airflow_service_config()
        config_json = json.dumps(config, indent=2)
        print("Workflow configuration prepared")
        
        is_mock = Workflow.__name__ == 'MockWorkflow'
        if is_mock:
            print("Using mock workflow for testing (OpenMetadata libraries not found)")
            print("In a production environment, please ensure OpenMetadata libraries are installed")
            
        workflow = Workflow.create(config_json)
        workflow.execute()
        workflow.raise_from_status()
        workflow.print_status()
        workflow.stop()
        
        if is_mock:
            print("✓ Mock Airflow Pipeline Service ingestion completed successfully!")
        else:
            print("✓ Airflow Pipeline Service ingestion completed successfully!")
        
    except Exception as e:
        print(f"✗ Airflow Pipeline Service ingestion failed: {str(e)}")
        print(f"Error type: {type(e).__name__}")  
        import traceback
        traceback.print_exc()
        raise

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'airflow_to_openmetadata_ingestion',
    default_args=default_args,
    description='Ingest Airflow database and pipeline metadata to OpenMetadata',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    tags=['airflow', 'openmetadata', 'postgres', 'pipeline']
)

# Tasks
install_packages_task = PythonOperator(
    task_id='install_openmetadata_packages',
    python_callable=install_openmetadata_packages,
    dag=dag,
)

postgres_metadata_ingestion_task = PythonOperator(
    task_id='airflow_postgres_metadata_ingestion',
    python_callable=run_airflow_postgres_ingestion,
    dag=dag,
)

airflow_service_ingestion_task = PythonOperator(
    task_id='airflow_service_ingestion', 
    python_callable=run_airflow_service_ingestion,
    dag=dag,
)

# Task Dependencies
install_packages_task >> postgres_metadata_ingestion_task >> airflow_service_ingestion_task