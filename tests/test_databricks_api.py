"""
Comprehensive API tests for Databricks REST APIs

Tests cover:
- Cluster management
- Job operations
- DBFS file operations
- SQL endpoints
- Workspace operations
"""

import requests
import json
import time
import pytest
from typing import Dict, Optional
import os
from datetime import datetime


class DatabricksAPIClient:
    """Client for Databricks REST API"""

    def __init__(self, host: str, token: str):
        """
        Initialize Databricks API client

        Args:
            host: Databricks workspace URL (e.g., https://adb-xxx.azuredatabricks.net)
            token: Personal access token
        """
        self.host = host.rstrip('/')
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        self.base_url = f"{self.host}/api/2.0"

    def _request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Dict:
        """Make API request"""
        url = f"{self.base_url}/{endpoint}"
        response = requests.request(
            method=method,
            url=url,
            headers=self.headers,
            json=data
        )
        response.raise_for_status()
        return response.json() if response.text else {}


@pytest.fixture
def api_client():
    """Fixture for API client"""
    host = os.getenv("DATABRICKS_HOST", "https://your-workspace.cloud.databricks.com")
    token = os.getenv("DATABRICKS_TOKEN", "your-token")
    return DatabricksAPIClient(host, token)


class TestClusterManagement:
    """Tests for Cluster Management API"""

    def test_list_clusters(self, api_client):
        """Test listing all clusters"""
        response = api_client._request("GET", "clusters/list")

        assert "clusters" in response or response == {}
        print(f"Found {len(response.get('clusters', []))} clusters")

    def test_create_cluster(self, api_client):
        """Test cluster creation"""
        cluster_config = {
            "cluster_name": f"test-cluster-{int(time.time())}",
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 2,
            "autotermination_minutes": 30,
            "spark_conf": {
                "spark.databricks.delta.preview.enabled": "true"
            }
        }

        response = api_client._request("POST", "clusters/create", cluster_config)

        assert "cluster_id" in response
        cluster_id = response["cluster_id"]
        print(f"Created cluster: {cluster_id}")

        # Cleanup
        api_client._request("POST", "clusters/delete", {"cluster_id": cluster_id})

    def test_get_cluster_info(self, api_client):
        """Test getting cluster information"""
        # First, list clusters to get an ID
        clusters_response = api_client._request("GET", "clusters/list")
        clusters = clusters_response.get("clusters", [])

        if clusters:
            cluster_id = clusters[0]["cluster_id"]
            response = api_client._request("GET", "clusters/get", {"cluster_id": cluster_id})

            assert "cluster_id" in response
            assert "state" in response
            print(f"Cluster {cluster_id} state: {response['state']}")

    def test_start_cluster(self, api_client):
        """Test starting a terminated cluster"""
        # Create a terminated cluster first
        cluster_config = {
            "cluster_name": f"test-start-cluster-{int(time.time())}",
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 1,
            "autotermination_minutes": 30
        }

        create_response = api_client._request("POST", "clusters/create", cluster_config)
        cluster_id = create_response["cluster_id"]

        # Wait a bit for cluster to be created
        time.sleep(5)

        # Start the cluster
        api_client._request("POST", "clusters/start", {"cluster_id": cluster_id})

        # Verify it's starting
        status = api_client._request("GET", "clusters/get", {"cluster_id": cluster_id})
        assert status["state"] in ["PENDING", "RUNNING", "RESTARTING"]

        print(f"Cluster {cluster_id} is starting")

        # Cleanup
        api_client._request("POST", "clusters/delete", {"cluster_id": cluster_id})

    def test_cluster_events(self, api_client):
        """Test getting cluster events"""
        clusters_response = api_client._request("GET", "clusters/list")
        clusters = clusters_response.get("clusters", [])

        if clusters:
            cluster_id = clusters[0]["cluster_id"]
            response = api_client._request("POST", "clusters/events", {
                "cluster_id": cluster_id,
                "order": "DESC",
                "limit": 10
            })

            assert "events" in response or "total_count" in response
            print(f"Retrieved cluster events for {cluster_id}")


class TestJobOperations:
    """Tests for Jobs API"""

    def test_list_jobs(self, api_client):
        """Test listing all jobs"""
        response = api_client._request("GET", "jobs/list")

        assert "jobs" in response or response == {}
        print(f"Found {len(response.get('jobs', []))} jobs")

    def test_create_job(self, api_client):
        """Test job creation"""
        job_config = {
            "name": f"test-job-{int(time.time())}",
            "tasks": [
                {
                    "task_key": "test_task",
                    "notebook_task": {
                        "notebook_path": "/Workspace/test_notebook",
                        "base_parameters": {}
                    },
                    "new_cluster": {
                        "spark_version": "13.3.x-scala2.12",
                        "node_type_id": "i3.xlarge",
                        "num_workers": 1
                    }
                }
            ],
            "timeout_seconds": 3600,
            "max_concurrent_runs": 1
        }

        response = api_client._request("POST", "jobs/create", job_config)

        assert "job_id" in response
        job_id = response["job_id"]
        print(f"Created job: {job_id}")

        # Cleanup
        api_client._request("POST", "jobs/delete", {"job_id": job_id})

    def test_run_job(self, api_client):
        """Test running a job"""
        # Create a simple job
        job_config = {
            "name": f"test-run-job-{int(time.time())}",
            "tasks": [
                {
                    "task_key": "test_task",
                    "spark_python_task": {
                        "python_file": "dbfs:/tmp/test.py"
                    },
                    "new_cluster": {
                        "spark_version": "13.3.x-scala2.12",
                        "node_type_id": "i3.xlarge",
                        "num_workers": 1
                    }
                }
            ]
        }

        create_response = api_client._request("POST", "jobs/create", job_config)
        job_id = create_response["job_id"]

        # Run the job
        run_response = api_client._request("POST", "jobs/run-now", {"job_id": job_id})

        assert "run_id" in run_response
        run_id = run_response["run_id"]
        print(f"Job run started: {run_id}")

        # Cleanup
        api_client._request("POST", "jobs/delete", {"job_id": job_id})

    def test_get_job_runs(self, api_client):
        """Test getting job run history"""
        jobs_response = api_client._request("GET", "jobs/list")
        jobs = jobs_response.get("jobs", [])

        if jobs:
            job_id = jobs[0]["job_id"]
            response = api_client._request("GET", "jobs/runs/list", {
                "job_id": job_id,
                "limit": 10
            })

            print(f"Retrieved runs for job {job_id}")


class TestDBFS:
    """Tests for DBFS (Databricks File System) API"""

    def test_list_dbfs(self, api_client):
        """Test listing DBFS directories"""
        response = api_client._request("GET", "dbfs/list", {"path": "/"})

        assert "files" in response
        print(f"Found {len(response['files'])} items in DBFS root")

    def test_create_directory(self, api_client):
        """Test creating DBFS directory"""
        test_path = f"/tmp/test-dir-{int(time.time())}"

        response = api_client._request("POST", "dbfs/mkdirs", {"path": test_path})

        # Verify directory exists
        list_response = api_client._request("GET", "dbfs/list", {"path": "/tmp"})
        dir_names = [f["path"] for f in list_response["files"]]

        assert test_path in dir_names
        print(f"Created directory: {test_path}")

        # Cleanup
        api_client._request("POST", "dbfs/delete", {"path": test_path, "recursive": True})

    def test_upload_file(self, api_client):
        """Test uploading file to DBFS"""
        test_path = f"/tmp/test-file-{int(time.time())}.txt"
        content = "Test content for DBFS upload"

        # Upload file (using base64 encoding)
        import base64
        encoded_content = base64.b64encode(content.encode()).decode()

        # Create file
        api_client._request("POST", "dbfs/put", {
            "path": test_path,
            "contents": encoded_content,
            "overwrite": True
        })

        # Verify file exists
        status = api_client._request("GET", "dbfs/get-status", {"path": test_path})

        assert status["path"] == test_path
        print(f"Uploaded file: {test_path}")

        # Cleanup
        api_client._request("POST", "dbfs/delete", {"path": test_path})

    def test_read_file(self, api_client):
        """Test reading file from DBFS"""
        test_path = f"/tmp/test-read-{int(time.time())}.txt"
        content = "Test content for reading"

        # Upload file first
        import base64
        encoded_content = base64.b64encode(content.encode()).decode()
        api_client._request("POST", "dbfs/put", {
            "path": test_path,
            "contents": encoded_content,
            "overwrite": True
        })

        # Read file
        read_response = api_client._request("GET", "dbfs/read", {
            "path": test_path,
            "length": 1000000
        })

        assert "data" in read_response
        decoded_content = base64.b64decode(read_response["data"]).decode()
        assert decoded_content == content

        print(f"Successfully read file: {test_path}")

        # Cleanup
        api_client._request("POST", "dbfs/delete", {"path": test_path})


class TestSQLEndpoints:
    """Tests for SQL Endpoints (Warehouses) API"""

    def test_list_warehouses(self, api_client):
        """Test listing SQL warehouses"""
        # Note: SQL API uses /api/2.0/sql/warehouses
        url = f"{api_client.host}/api/2.0/sql/warehouses"
        response = requests.get(url, headers=api_client.headers)

        if response.status_code == 200:
            data = response.json()
            warehouses = data.get("warehouses", [])
            print(f"Found {len(warehouses)} SQL warehouses")

    def test_create_warehouse(self, api_client):
        """Test creating SQL warehouse"""
        warehouse_config = {
            "name": f"test-warehouse-{int(time.time())}",
            "cluster_size": "2X-Small",
            "max_num_clusters": 1,
            "auto_stop_mins": 30,
            "enable_photon": True,
            "enable_serverless_compute": False
        }

        url = f"{api_client.host}/api/2.0/sql/warehouses"
        response = requests.post(
            url,
            headers=api_client.headers,
            json=warehouse_config
        )

        if response.status_code == 200:
            data = response.json()
            assert "id" in data
            warehouse_id = data["id"]
            print(f"Created warehouse: {warehouse_id}")

            # Cleanup
            delete_url = f"{api_client.host}/api/2.0/sql/warehouses/{warehouse_id}"
            requests.delete(delete_url, headers=api_client.headers)


class TestWorkspace:
    """Tests for Workspace API"""

    def test_list_workspace(self, api_client):
        """Test listing workspace items"""
        response = api_client._request("GET", "workspace/list", {"path": "/"})

        assert "objects" in response or response == {}
        print(f"Found {len(response.get('objects', []))} workspace items")

    def test_get_workspace_status(self, api_client):
        """Test getting workspace object status"""
        response = api_client._request("GET", "workspace/get-status", {"path": "/"})

        assert "path" in response
        assert "object_type" in response
        print(f"Workspace root type: {response['object_type']}")


class TestIntegration:
    """Integration tests combining multiple APIs"""

    def test_end_to_end_job_execution(self, api_client):
        """Test complete job creation, execution, and monitoring"""
        timestamp = int(time.time())

        # 1. Create cluster
        cluster_config = {
            "cluster_name": f"integration-test-{timestamp}",
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 1,
            "autotermination_minutes": 30
        }

        cluster_response = api_client._request("POST", "clusters/create", cluster_config)
        cluster_id = cluster_response["cluster_id"]
        print(f"1. Created cluster: {cluster_id}")

        # 2. Wait for cluster to start
        max_wait = 300  # 5 minutes
        start_time = time.time()
        while time.time() - start_time < max_wait:
            status = api_client._request("GET", "clusters/get", {"cluster_id": cluster_id})
            if status["state"] == "RUNNING":
                print("2. Cluster is running")
                break
            time.sleep(10)

        # 3. Create a test script in DBFS
        test_script = """
print("Integration test execution")
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.range(100)
print(f"Created DataFrame with {df.count()} rows")
"""
        import base64
        encoded_script = base64.b64encode(test_script.encode()).decode()
        script_path = f"/tmp/integration-test-{timestamp}.py"

        api_client._request("POST", "dbfs/put", {
            "path": script_path,
            "contents": encoded_script,
            "overwrite": True
        })
        print(f"3. Created test script: {script_path}")

        # 4. Create and run job
        job_config = {
            "name": f"integration-test-job-{timestamp}",
            "tasks": [
                {
                    "task_key": "integration_task",
                    "spark_python_task": {
                        "python_file": f"dbfs:{script_path}"
                    },
                    "existing_cluster_id": cluster_id
                }
            ]
        }

        job_response = api_client._request("POST", "jobs/create", job_config)
        job_id = job_response["job_id"]
        print(f"4. Created job: {job_id}")

        run_response = api_client._request("POST", "jobs/run-now", {"job_id": job_id})
        run_id = run_response["run_id"]
        print(f"5. Started job run: {run_id}")

        # 6. Cleanup
        print("6. Cleaning up resources...")
        api_client._request("POST", "jobs/delete", {"job_id": job_id})
        api_client._request("POST", "dbfs/delete", {"path": script_path})
        api_client._request("POST", "clusters/delete", {"cluster_id": cluster_id})

        print("Integration test completed successfully!")


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "-s"])
