import os
import json
import subprocess
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.configuration import conf

def install_openmetadata_packages():
    try:
        python_executable = sys.executable
        print(f"Using Python: {python_executable}")
        
        packages = [
            "openmetadata-ingestion[oracle]==1.3.3",
            "cx_Oracle==8.3.0",
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
 

def get_workflow_class_safe():
    try:
        saved_path = os.environ.get('OPENMETADATA_WORKFLOW_PATH')
        if saved_path:
            try:
                module = __import__(saved_path, fromlist=['Workflow'])
                return getattr(module, 'Workflow')
            except (ImportError, AttributeError):
                pass
        
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
        
        raise ImportError("No valid Workflow class found in any module!")
        
    except Exception as e:
        print(f"Error getting Workflow class: {e}")
        raise

ORACLE_CONNECTION = {
    "serviceName": "orclpdb",
    "hostPort": "192.168.1.200:1525",
    "username": "BSHKT_BSH", 
    "password": "Inda1234",
    "database": "BSHKT_BSH",
    "includeViews": True
}

OPENMETADATA_CONNECTION = {
    "hostPort": "http://192.168.1.10:8585",
    "authProvider": "no-auth"
}

def get_oracle_metadata_config():
    return {
        "source": {
            "type": "oracle",
            "serviceName": ORACLE_CONNECTION["serviceName"],
            "serviceConnection": {
                "config": {
                    "type": "Oracle",
                    "hostPort": ORACLE_CONNECTION["hostPort"],
                    "username": ORACLE_CONNECTION["username"],
                    "password": ORACLE_CONNECTION["password"],
                    "database": ORACLE_CONNECTION["database"]
                }
            },
            "sourceConfig": {
                "config": {
                    "type": "DatabaseMetadata",
                    "includeViews": ORACLE_CONNECTION["includeViews"],
                    "includeTables": True,
                    "schemaFilterPattern": {
                        "includes": [".*"]
                    },
                    "tableFilterPattern": {
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

def get_oracle_lineage_config():
    return {
        "source": {
            "type": "oracle-lineage", 
            "serviceName": ORACLE_CONNECTION["serviceName"],
            "serviceConnection": {
                "config": {
                    "type": "Oracle",
                    "hostPort": ORACLE_CONNECTION["hostPort"],
                    "username": ORACLE_CONNECTION["username"],
                    "password": ORACLE_CONNECTION["password"],
                    "database": ORACLE_CONNECTION["database"]
                }
            },
            "sourceConfig": {
                "config": {
                    "type": "DatabaseLineage",
                    "queryLogDuration": 1,
                    "resultLimit": 1000
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

def run_metadata_ingestion():
    try:
        print("Starting Oracle metadata ingestion...")
        Workflow = get_workflow_class_safe()
        config = get_oracle_metadata_config()
        config_json = json.dumps(config, indent=2)
        print("Workflow configuration:")
        print(config_json)
        workflow = Workflow.create(config_json)
        workflow.execute()
        workflow.raise_from_status()
        workflow.print_status() 
        workflow.stop()
        print("✓ Metadata ingestion completed successfully!")
        
    except Exception as e:
        print(f"✗ Metadata ingestion failed: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        raise

def run_lineage_ingestion():
    try:
        print("Starting Oracle lineage ingestion...")
        Workflow = get_workflow_class_safe()
        config = get_oracle_lineage_config()
        config_json = json.dumps(config, indent=2)
        print("Workflow configuration:")
        print(config_json)
        workflow = Workflow.create(config_json)
        workflow.execute()
        workflow.raise_from_status()
        workflow.print_status()
        workflow.stop()
        print("✓ Lineage ingestion completed successfully!")
        
    except Exception as e:
        print(f"✗ Lineage ingestion failed: {str(e)}")
        print(f"Error type: {type(e).__name__}")  
        import traceback
        traceback.print_exc()
        raise

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
    'oracle_openmetadata_ingestion',
    default_args=default_args,
    description='Oracle metadata and lineage ingestion to OpenMetadata',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['oracle', 'openmetadata', 'ingestion']
)

install_packages_task = PythonOperator(
    task_id='install_openmetadata_packages',
    python_callable=install_openmetadata_packages,
    dag=dag,
)


metadata_ingestion_task = PythonOperator(
    task_id='oracle_metadata_ingestion',
    python_callable=run_metadata_ingestion,
    dag=dag,
)

lineage_ingestion_task = PythonOperator(
    task_id='oracle_lineage_ingestion', 
    python_callable=run_lineage_ingestion,
    dag=dag,
)

install_packages_task  >> metadata_ingestion_task >> lineage_ingestion_task
