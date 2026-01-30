.PHONY: help install test lint format validate deploy clean

help:
	@echo "Databricks Delta Lake Migration - Available Commands:"
	@echo ""
	@echo "  make install     - Install Python dependencies"
	@echo "  make test        - Run API tests"
	@echo "  make lint        - Lint Python code"
	@echo "  make format      - Format Python code"
	@echo "  make validate    - Validate Terraform configuration"
	@echo "  make plan        - Plan Terraform infrastructure"
	@echo "  make deploy      - Deploy infrastructure with Terraform"
	@echo "  make benchmark   - Run performance benchmarks"
	@echo "  make clean       - Clean temporary files"
	@echo ""

install:
	pip install -r requirements.txt
	pip install -r tests/requirements.txt

test:
	cd tests && pytest test_databricks_api.py -v

test-coverage:
	cd tests && pytest test_databricks_api.py --cov=. --cov-report=html

lint:
	flake8 src/ migration/ tests/ --max-line-length=120
	mypy src/ --ignore-missing-imports

format:
	black src/ migration/ tests/ --line-length=120

validate:
	cd infrastructure && terraform fmt -check
	cd infrastructure && terraform validate

plan:
	cd infrastructure && terraform plan

deploy:
	cd infrastructure && terraform apply

benchmark:
	cd benchmarks && ./run_benchmarks.sh

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	rm -f benchmark_report_*.json

setup-env:
	@echo "Setting up environment variables..."
	@echo "Please set the following environment variables:"
	@echo "  export DATABRICKS_HOST='https://your-workspace.cloud.databricks.com'"
	@echo "  export DATABRICKS_TOKEN='your-token'"
	@echo "  export AWS_REGION='us-west-2'"

check-env:
	@test -n "$$DATABRICKS_HOST" || (echo "Error: DATABRICKS_HOST not set" && exit 1)
	@test -n "$$DATABRICKS_TOKEN" || (echo "Error: DATABRICKS_TOKEN not set" && exit 1)
	@echo "✓ Environment variables configured"
