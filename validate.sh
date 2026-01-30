#!/bin/bash

# Validation script for Databricks Delta Lake Migration project

set -e

echo "========================================="
echo "Databricks Delta Lake Migration"
echo "Validation Script"
echo "========================================="
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print success
success() {
    echo -e "${GREEN}✓${NC} $1"
}

# Function to print error
error() {
    echo -e "${RED}✗${NC} $1"
}

# Function to print info
info() {
    echo -e "${YELLOW}ℹ${NC} $1"
}

echo "1. Checking directory structure..."
required_dirs=("infrastructure" "notebooks" "src" "tests" "migration" "benchmarks")
for dir in "${required_dirs[@]}"; do
    if [ -d "$dir" ]; then
        success "Directory exists: $dir"
    else
        error "Directory missing: $dir"
        exit 1
    fi
done
echo ""

echo "2. Checking Python files syntax..."
python_files=(
    "src/data_processor.py"
    "src/delta_utils.py"
    "src/config.py"
    "migration/migrate_from_rdbms.py"
    "migration/migrate_from_hdfs.py"
    "migration/migrate_from_s3.py"
    "tests/test_databricks_api.py"
    "benchmarks/delta_performance_benchmark.py"
)

for file in "${python_files[@]}"; do
    if python3 -m py_compile "$file" 2>/dev/null; then
        success "Syntax valid: $file"
    else
        error "Syntax error: $file"
        exit 1
    fi
done
echo ""

echo "3. Validating Terraform configuration..."
cd infrastructure
if terraform validate > /dev/null 2>&1; then
    success "Terraform configuration is valid"
else
    error "Terraform validation failed"
    terraform validate
    exit 1
fi
cd ..
echo ""

echo "4. Checking required files..."
required_files=(
    "README.md"
    ".gitignore"
    "requirements.txt"
    "Makefile"
    "infrastructure/main.tf"
    "infrastructure/variables.tf"
    "infrastructure/iam.tf"
)

for file in "${required_files[@]}"; do
    if [ -f "$file" ]; then
        success "File exists: $file"
    else
        error "File missing: $file"
        exit 1
    fi
done
echo ""

echo "5. Checking notebooks..."
notebooks=(
    "notebooks/01_bronze_ingestion.py"
    "notebooks/02_silver_transformation.py"
    "notebooks/03_gold_aggregation.py"
    "notebooks/04_acid_operations.py"
    "notebooks/05_time_travel_demo.py"
)

for notebook in "${notebooks[@]}"; do
    if [ -f "$notebook" ]; then
        success "Notebook exists: $notebook"
    else
        error "Notebook missing: $notebook"
        exit 1
    fi
done
echo ""

echo "6. Checking executable scripts..."
if [ -x "benchmarks/run_benchmarks.sh" ]; then
    success "Benchmark script is executable"
else
    error "Benchmark script is not executable"
    exit 1
fi
echo ""

echo "7. Checking Git repository..."
if [ -d ".git" ]; then
    success "Git repository initialized"

    # Check remote
    if git remote -v | grep -q "origin"; then
        success "Git remote 'origin' configured"
        REPO_URL=$(git remote get-url origin)
        info "Repository: $REPO_URL"
    else
        error "Git remote 'origin' not configured"
    fi
else
    error "Not a Git repository"
    exit 1
fi
echo ""

echo "8. Checking documentation..."
if grep -q "# Databricks Delta Lake Migration" README.md; then
    success "README has proper title"
else
    error "README missing proper title"
fi

if grep -q "Migration Guide" README.md; then
    success "README contains Migration Guide"
else
    error "README missing Migration Guide"
fi

if grep -q "Best Practices" README.md; then
    success "README contains Best Practices"
else
    error "README missing Best Practices"
fi
echo ""

echo "========================================="
echo -e "${GREEN}All validations passed!${NC}"
echo "========================================="
echo ""

info "Next steps:"
echo "  1. Configure Terraform variables (infrastructure/terraform.tfvars)"
echo "  2. Set up Databricks workspace credentials"
echo "  3. Run: terraform init && terraform apply"
echo "  4. Upload notebooks to Databricks workspace"
echo "  5. Execute migration scripts"
echo ""

info "For detailed instructions, see README.md"
