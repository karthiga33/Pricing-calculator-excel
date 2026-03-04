# AWS Pricing Calculator

A Streamlit-based web application that generates detailed AWS cost estimation reports in Excel format using AWS Bedrock AI.

## Features

- Upload CSV files with AWS service cost data
- Generate comprehensive Excel reports with:
  - EC2 instance specifications (vCPU, RAM)
  - Cost breakdowns in USD and INR
  - Service descriptions using AWS Bedrock
  - Best practices for cost optimization
- Interactive web interface
- Automated currency conversion

## Prerequisites

- Python 3.8+
- AWS Account with Bedrock access
- AWS credentials configured

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd GenAI-first
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure AWS credentials:
```bash
aws configure
```
Ensure you have access to AWS Bedrock in us-east-1 region.

## Usage

1. Run the Streamlit app:
```bash
streamlit run f.py
```

2. Open your browser (usually http://localhost:8501)

3. Upload a CSV file with columns:
   - Service
   - Monthly Cost
   - Configuration Summary

4. Fill in the required parameters:
   - Customer Name
   - USD to INR Exchange Rate
   - AWS Region
   - Pricing Link (optional)

5. Click "Generate Cost Report" and download the Excel file

## CSV Format

The CSV should contain AWS service cost data with the first 7 rows skipped during processing. Required columns:
- **Service**: AWS service name
- **Monthly Cost**: Cost in USD
- **Configuration Summary**: Service configuration details

## Configuration

- Default exchange rate: 85.50 INR/USD
- Default region: US East (N. Virginia)
- AWS Bedrock model: us.amazon.nova-lite-v1:0

## Notes

- Ensure AWS credentials have permissions for Bedrock
- Output files are temporarily stored and cleaned up automatically
- The application uses AWS Bedrock for AI-powered descriptions
