import json
import boto3
import logging
import pandas as pd
import io
import re
from datetime import datetime
import time
import urllib.parse
import os

# Set up logging with structured format
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration from environment variables
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET", "biocon-data-extraction")
OUTPUT_FILE_KEY = os.environ.get("OUTPUT_FILE_KEY", "MMF/output-excel-file/MMF_data.xlsx")
TEXT_OUTPUT_PREFIX = os.environ.get("TEXT_OUTPUT_PREFIX", "MMF/text-extraction/")
INPUT_PREFIX = os.environ.get("INPUT_PREFIX", "MMF/input-files/")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "biocon-data-extraction-db")
MODEL_NOVA_PRO = os.environ.get("MODEL_NOVA_PRO", "amazon.nova-pro-v1:0")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# AWS Clients
s3_client = boto3.client("s3")
dynamodb_client = boto3.client("dynamodb")
dynamodb_resource = boto3.resource("dynamodb")
bedrock_client = boto3.client("bedrock-runtime", region_name=AWS_REGION)
textract_client = boto3.client("textract", region_name=AWS_REGION)

# Critical query fields for Textract
CRITICAL_QUERY_FIELDS = [
    {"Text": "Batch Number", "Alias": "BN"},
    {"Text": "Product Name", "Alias": "PN"},
    {"Text": "Product Code", "Alias": "PC"},
    {"Text": "Dispatch Quantity", "Alias": "DQ"},
    {"Text": "Manufacturing Date", "Alias": "MD"},
    {"Text": "Packing Start Date", "Alias": "PSD"},
    {"Text": "Material Quantity", "Alias": "MQ"},
    {"Text": "Quantity to be packed", "Alias": "QTP"},
    {"Text": "Number of Containers", "Alias": "NC"},
    {"Text": "MRCIS Number", "Alias": "MN"},
    {"Text": "Required Quantity", "Alias": "RQ"},
    {"Text": "Received Quantity", "Alias": "RCQ"},
    {"Text": "Weighing Balance Code", "Alias": "WBC"}
]

# Required details for extraction (155 columns)
DETAILS_TO_EXTRACT = [
    "Batch Number", "Product Name", "Product Code", "Dispatch Qty", "Manufacturing Date",
    "Packing Start Date", "Material Qty", "Quantity to packed", "No of Container", "MRICS No",
    "Required Qty", "Received Qty", "Weighing Balance Code",
] + [
    f"WMS Finished Product Gross Weight-{i}" for i in range(1, 39)
] + [
    f"WMS Finished Product Tare Weight-{i}" for i in range(1, 39)
] + [
    f"WMS Finished Product Net Weight-{i}" for i in range(1, 39)
] + [
    f"Current Gross Weight-{i}" for i in range(1, 39)
] + [
    "File Name", "S3 File Path"
]

def validate_field(field, value):
    """Centralized validation for field values."""
    if not value:
        return None
    if "Weight" in field or "Qty" in field:
        try:
            float_value = float(value)
            return str(float_value) if float_value >= 0 else None
        except ValueError:
            logger.warning(f"Invalid weight/quantity format for {field}: {value}")
            return None
    elif "Date" in field:
        try:
            if re.match(r"^[A-Za-z]{3}-\d{2}$", value, re.IGNORECASE):
                value = f"01-{value}"
                datetime.strptime(value, "%d-%b-%y")
            else:
                datetime.strptime(value, "%d/%m/%Y")
                value = datetime.strptime(value, "%d/%m/%Y").strftime("%d-%m-%Y")
            return value
        except ValueError:
            logger.warning(f"Invalid date format for {field}: {value}")
            return None
    elif field == "No of Container":
        try:
            int_value = int(value)
            return str(int_value) if int_value >= 0 else None
        except ValueError:
            logger.warning(f"Invalid container count format: {value}")
            return None
    return str(value)

def check_s3_object_exists(bucket_name, file_key, retries=3, delay=5):
    """Check if the S3 object exists with retries."""
    for attempt in range(retries):
        try:
            s3_client.head_object(Bucket=bucket_name, Key=file_key)
            return True
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            logger.error(f"Attempt {attempt + 1}/{retries} - Error checking S3 object s3://{bucket_name}/{file_key}: {str(e)}")
            if attempt < retries - 1:
                time.sleep(delay)
    logger.error(f"Failed to check S3 object after {retries} attempts: s3://{bucket_name}/{file_key}")
    return False

def check_dynamodb_record(s3_file_path):
    """Check if the file has already beenMV processed in DynamoDB and validate data quality."""
    try:
        response = dynamodb_client.get_item(
            TableName=DYNAMODB_TABLE,
            Key={'S3FilePath': {'S': s3_file_path}}
        )
        if 'Item' in response:
            item = response['Item']
            status = item.get('ProcessingStatus', {}).get('S', '')
            error_message = item.get('ErrorMessage', {}).get('S', '')
            structured_data = item.get('StructuredData', {}).get('M', {})
            if not structured_data:
                logger.info(f"Empty StructuredData for {s3_file_path}. Forcing reprocessing.")
                return False, "Empty StructuredData in previous run"
            non_empty_values = sum(1 for value in structured_data.values() if value.get('S', '') != '')
            if status == 'SUCCESS' and non_empty_values > 10:
                logger.info(f"File already successfully processed with sufficient data: {s3_file_path}")
                return True, None
            elif status == 'SUCCESS':
                logger.info(f"File processed but contains insufficient data: {s3_file_path}. Forcing reprocessing.")
                return False, "Incomplete data in previous run"
            elif status == 'FAILED':
                logger.info(f"Retrying FAILED record for {s3_file_path}: {error_message}")
                return False, error_message
            return True, error_message
        return False, None
    except Exception as e:
        logger.error(f"Error checking DynamoDB for {s3_file_path}: {str(e)}")
        return False, None

def save_to_dynamodb(s3_file_path, file_name, structured_data, status="SUCCESS", error_message=None):
    """Save processed file metadata to DynamoDB."""
    try:
        table = dynamodb_resource.Table(DYNAMODB_TABLE)
        item = {
            'S3FilePath': s3_file_path,
            'FileName': file_name,
            'ProcessedAt': datetime.utcnow().isoformat(),
            'ProcessingStatus': status,
            'StructuredData': {key: {'S': str(value)} for key, value in structured_data.items() if value}
        }
        if error_message:
            item['ErrorMessage'] = error_message
        table.put_item(Item=item)
        logger.info(f"Saved to DynamoDB: {s3_file_path}, Status: {status}")
    except Exception as e:
        logger.error(f"Error saving to DynamoDB for {s3_file_path}: {str(e)}")
        raise

def get_all_dynamodb_records():
    """Retrieve all processed records from DynamoDB."""
    try:
        table = dynamodb_resource.Table(DYNAMODB_TABLE)
        response = table.scan()
        items = response.get('Items', [])
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))
        for item in items:
            s3_file_path = item.get('S3FilePath', {}).get('S', '') if isinstance(item.get('S3FilePath'), dict) else item.get('S3FilePath', '')
            if s3_file_path.startswith("biocon-data-extraction/"):
                item['S3FilePath'] = {'S': s3_file_path.replace("biocon-data-extraction/", "s3://biocon-data-extraction/")}
            if s3_file_path.startswith("s3://s3://"):
                item['S3FilePath'] = {'S': s3_file_path.replace("s3://s3://", "s3://")}
            structured_data = item.get('StructuredData', {})
            if isinstance(structured_data, str):
                try:
                    parsed_data = json.loads(structured_data)
                    item['StructuredData'] = {'M': {k: {'S': str(v)} for k, v in parsed_data.items()}}
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON StructuredData for {s3_file_path}, setting to empty dict")
                    item['StructuredData'] = {'M': {}}
            elif isinstance(structured_data, dict) and 'M' not in structured_data:
                logger.warning(f"Invalid StructuredData format for {s3_file_path}, setting to empty dict")
                item['StructuredData'] = {'M': {}}
            elif not isinstance(structured_data.get('M', {}), dict):
                logger.warning(f"StructuredData 'M' is not a dict for {s3_file_path}, setting to empty dict")
                item['StructuredData'] = {'M': {}}
        return items
    except Exception as e:
        logger.error(f"Error retrieving DynamoDB records: {str(e)}")
        return []

def reconstruct_table_from_text(text):
    """Reconstruct table data from raw text if Textract tables are empty."""
    table_data = []
    weight_pattern = re.compile(
        r"(\d+)\s*x\s*(\d+\.\d{1,3})(?:\s*kg)?",
        re.MULTILINE | re.IGNORECASE
    )
    unmatched_lines = []
    lines = text.split("\n")
    for line in lines:
        line = line.strip()
        match = weight_pattern.search(line)
        if match:
            container_num = match.group(1)
            net_weight = match.group(2)
            try:
                float(net_weight)
                table_data.append({
                    "Container No.": container_num,
                    "Gross wt.(kg)": "",
                    "Tare wt.(kg)": "",
                    "Net wt.(kg)": net_weight,
                    "Current Gross wt.(kg)": ""
                })
            except ValueError:
                logger.warning(f"Invalid weight format in line: {line}")
                unmatched_lines.append(line)
        elif line and "container" in line.lower():
            unmatched_lines.append(line)
    if unmatched_lines:
        logger.info(f"Unmatched lines during table reconstruction: {unmatched_lines[:10]}")
    if not table_data:
        logger.warning("No table data reconstructed from raw text")
    table_text = "\nReconstructed Table:\n"
    for row_idx, row in enumerate(table_data, 1):
        table_text += f"Row {row_idx}: {row['Container No.']} | {row['Gross wt.(kg)']} | {row['Tare wt.(kg)']} | {row['Net wt.(kg)']} | {row['Current Gross wt.(kg)']}\n"
    logger.info(f"Reconstructed table data: {table_text[:1000]}...")
    return table_text, table_data

def format_table_data(blocks, raw_text):
    """Format Textract table and query data, falling back to text reconstruction if tables are empty."""
    tables = []
    current_table = []
    queries = {}
    for block in blocks:
        if block['BlockType'] == 'TABLE':
            current_table = []
            tables.append(current_table)
        elif block['BlockType'] == 'CELL' and current_table is not None:
            row_index = block.get('RowIndex', 0)
            col_index = block.get('ColumnIndex', 0)
            text = block.get('Text', '') or ''
            while len(current_table) < row_index:
                current_table.append([])
            while len(current_table[row_index - 1]) < col_index:
                current_table[row_index - 1].append('')
            current_table[row_index - 1][col_index - 1] = text
        elif block['BlockType'] == 'QUERY_RESULT':
            query_text = block.get('Query', {}).get('Alias', '')
            answer = block.get('Text', '') or ''
            if query_text and answer != "Not Found":
                queries[query_text] = answer
    table_text = ""
    is_empty = all(all(cell == '' for cell in row) for table in tables for row in table)
    if is_empty:
        logger.warning("Textract tables are empty, reconstructing from raw text")
        table_text, _ = reconstruct_table_from_text(raw_text)
    else:
        for table_idx, table in enumerate(tables, 1):
            table_text += f"\nTable {table_idx}:\n"
            for row_idx, row in enumerate(table, 1):
                table_text += f"Row {row_idx}: {' | '.join(cell for cell in row)}\n"
            table_text += "\n"
    query_text = "\nQueries:\n" + "\n".join(f"{k}: {v}" for k, v in queries.items()) if queries else ""
    logger.info(f"Formatted table data: {table_text[:1000]}...")
    logger.info(f"Formatted query data: {query_text[:1000]}...")
    return table_text + query_text, queries

def extract_text_and_tables_with_textract(bucket_name, file_key, retries=3, delay=5):
    """Extract raw text, tables, and queries from PDF using Amazon Textract and save to S3."""
    extracted_text = ""
    file_name = file_key.split('/')[-1]
    base_name = file_name.rsplit('.', 1)[0]
    sanitized_base_name = re.sub(r'[^a-zA-Z0-9]', '', base_name)  # Sanitize file name
    txt_file_key = f"{TEXT_OUTPUT_PREFIX}{sanitized_base_name}.txt"

    try:
        if check_s3_object_exists(bucket_name, txt_file_key):
            logger.info(f"Text file already exists: s3://{bucket_name}/{txt_file_key}")
            response = s3_client.get_object(Bucket=bucket_name, Key=txt_file_key)
            content = response["Body"].read().decode('utf-8').strip()
            logger.info(f"Retrieved existing text file content: {content[:1000]}...")
            return content, []

        text_response = textract_client.start_document_text_detection(
            DocumentLocation={'S3Object': {'Bucket': bucket_name, 'Name': file_key}}
        )
        text_job_id = text_response['JobId']
        logger.info(f"Started Textract text detection job {text_job_id} for s3://{bucket_name}/{file_key}")

        table_response = textract_client.start_document_analysis(
            DocumentLocation={'S3Object': {'Bucket': bucket_name, 'Name': file_key}},
            FeatureTypes=['TABLES']
        )
        table_job_id = table_response['JobId']
        logger.info(f"Started Textract table analysis job {table_job_id} for s3://{bucket_name}/{file_key}")

        query_response = textract_client.start_document_analysis(
            DocumentLocation={'S3Object': {'Bucket': bucket_name, 'Name': file_key}},
            FeatureTypes=['QUERIES'],
            QueriesConfig={'Queries': CRITICAL_QUERY_FIELDS}
        )
        query_job_id = query_response['JobId']
        logger.info(f"Started Textract query analysis job {query_job_id} for s3://{bucket_name}/{file_key}")
        logger.info(f"Queries sent: {json.dumps(CRITICAL_QUERY_FIELDS, indent=2)[:1000]}...")

        max_attempts = 300
        query_blocks = []
        table_text = ""
        query_text = ""
        for job_id, job_type in [(text_job_id, 'text'), (table_job_id, 'table'), (query_job_id, 'query')]:
            attempt = 0
            while attempt < max_attempts:
                try:
                    if job_type == 'text':
                        response = textract_client.get_document_text_detection(JobId=job_id)
                    else:
                        response = textract_client.get_document_analysis(JobId=job_id)
                    status = response['JobStatus']
                    page_count = response.get('DocumentMetadata', {}).get('Pages', 'Unknown')
                    logger.info(f"Textract {job_type} job {job_id} status (pages: {page_count}): {status}")
                    if status in ['SUCCEEDED', 'FAILED']:
                        break
                    time.sleep(2)
                    attempt += 1
                except Exception as e:
                    logger.error(f"Error polling {job_type} job {job_id}: {str(e)}")
                    if attempt == max_attempts - 1:
                        raise Exception(f"{job_type.capitalize()} job polling failed: {str(e)}")

            if status == 'SUCCEEDED':
                blocks = response['Blocks']
                while 'NextToken' in response:
                    if job_type == 'text':
                        response = textract_client.get_document_text_detection(JobId=job_id, NextToken=response['NextToken'])
                    else:
                        response = textract_client.get_document_analysis(JobId=job_id, NextToken=response['NextToken'])
                    blocks.extend(response['Blocks'])
                
                if job_type == 'text':
                    for block in blocks:
                        if block['BlockType'] == 'LINE':
                            extracted_text += block.get('Text', '') + '\n'
                elif job_type == 'table':
                    table_text, _ = format_table_data(blocks, extracted_text)
                elif job_type == 'query':
                    _, batch_queries = format_table_data(blocks, extracted_text)
                    query_blocks.extend(blocks)
                    query_text += "\nQueries:\n" + "\n".join(f"{k}: {v}" for k, v in batch_queries.items()) if batch_queries else ""
            else:
                logger.warning(f"{job_type.capitalize()} job {job_id} failed with status: {status}")
                if job_type == 'query':
                    try:
                        error_response = textract_client.get_document_analysis(JobId=job_id)
                        error_message = error_response.get('StatusMessage', 'No details available')
                        logger.error(f"Query job {job_id} failed with details: {error_message}")
                    except Exception as e:
                        logger.error(f"Error retrieving failure details for query job {job_id}: {str(e)}")
                if job_type == 'table':
                    table_text, _ = reconstruct_table_from_text(extracted_text)
                elif job_type == 'query':
                    query_text += "\nQueries failed\n"

        combined_text = f"Raw Text:\n{extracted_text.strip()}\n\nTables:\n{table_text.strip()}\n\nQueries:\n{query_text.strip()}"
        logger.info(f"Combined text for s3://{bucket_name}/{file_key}: {combined_text[:1000]}...")

        s3_client.put_object(
            Bucket=bucket_name,
            Key=txt_file_key,
            Body=combined_text.encode('utf-8')
        )
        logger.info(f"Saved extracted text, tables, and queries to s3://{bucket_name}/{txt_file_key}")

        return combined_text.strip(), query_blocks

    except Exception as e:
        logger.error(f"Error extracting text, tables, and queries with Textract for s3://{bucket_name}/{file_key}: {str(e)}")
        if extracted_text:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=txt_file_key,
                Body=extracted_text.encode('utf-8')
            )
            logger.info(f"Saved partial text to s3://{bucket_name}/{txt_file_key}")
        raise

def extract_fields_from_text(text, query_blocks):
    """Extract critical fields from raw text and query results as a fallback."""
    structured_data = {}
    patterns = {
        "Batch Number": r"Batch Number[:\s]+([A-Za-z0-9]+)",
        "Product Name": r"PRODUCT NAME[:\s]+([A-Z\s]+)\s*PRODUCT CODE",
        "Product Code": r"PRODUCT CODE[:\s]+(\d+)",
        "Dispatch Qty": r"Quantity to be packed[:\s]+(\d+\.\d{1,3})\s*kg",
        "Manufacturing Date": r"Mfg Date[:\s]+(\d{2}/\d{2}/\d{4}|\w{3}-\d{2})",
        "Packing Start Date": r"Packing Start Date[:\s]+(\d{2}/\d{2}/\d{4}|\w{3}-\d{2})",
        "Material Qty": r"Material Qty[:\s]+(\d+\.\d{1,3})\s*kg",
        "Quantity to packed": r"Quantity to be packed[:\s]+(\d+\.\d{1,3})\s*kg",
        "No of Container": r"No\. of [Cc]ontainers[:\s]+(\d+)",
        "MRICS No": r"MRCIS No\.[:\s]+(\d+)",
        "Required Qty": r"Required qty\.[:\s]+(\d+\.\d{1,3})\s*kg",
        "Received Qty": r"Received qty\.[:\s]+(\d+\.\d{1,3})\s*kg",
        "Weighing Balance Code": r"Weighing Balance Code[:\s]+([A-Za-z0-9]+)"
    }
    
    # Extract from raw text
    for field, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
        if match:
            value = match.group(1).strip()
            validated_value = validate_field(field, value)
            if validated_value:
                structured_data[field] = validated_value

    # Extract weights from table reconstruction
    _, table_data = reconstruct_table_from_text(text)
    for row in table_data:
        try:
            container_num = int(row["Container No."])
            if 1 <= container_num <= 38:
                for weight_type in ["Gross wt.(kg)", "Tare wt.(kg)", "Net wt.(kg)", "Current Gross wt.(kg)"]:
                    field_name = f"WMS Finished Product {weight_type.split('.')[0]}-{container_num}"
                    if weight_type == "Current Gross wt.(kg)":
                        field_name = f"Current Gross Weight-{container_num}"
                    value = row[weight_type]
                    validated_value = validate_field(field_name, value)
                    if validated_value:
                        structured_data[field_name] = validated_value
        except (ValueError, KeyError):
            logger.warning(f"Invalid container data: {row}")
    
    # Extract from query results
    alias_map = {
        "Batch Number": "BN", "Product Name": "PN", "Product Code": "PC",
        "Dispatch Qty": "DQ", "Manufacturing Date": "MD", "Packing Start Date": "PSD",
        "Material Qty": "MQ", "Quantity to packed": "QTP", "No of Container": "NC",
        "MRICS No": "MN", "Required Qty": "RQ", "Received Qty": "RCQ",
        "Weighing Balance Code": "WBC"
    }
    for block in query_blocks:
        if block['BlockType'] == 'QUERY_RESULT':
            alias = block.get('Query', {}).get('Alias', '')
            answer = block.get('Text', '')
            for field, alias_key in alias_map.items():
                if alias == alias_key and answer:
                    validated_value = validate_field(field, answer)
                    if validated_value:
                        structured_data[field] = validated_value
    
    return structured_data

def send_to_bedrock_model(textract_text, query_blocks, bucket_name, file_key, retries=3, delay=5):
    """Sends extracted text and PDF to Amazon Nova Pro for structured data extraction."""
    # Generate JSON example with all fields explicitly listed
    json_example = "{\n"
    for i, field in enumerate(DETAILS_TO_EXTRACT[:-2]):  # Exclude File Name and S3 File Path
        json_example += f'    "{field}": "<value>"'
        if i < len(DETAILS_TO_EXTRACT[:-2]) - 1:
            json_example += ","
        json_example += "\n"
    json_example += "}"

    prompt = f"""You are an expert data extraction system specialized in processing complex PDFs, including scanned documents and handwritten text. Extract the following 153 fields exactly as present in the PDF content below. Follow these instructions precisely:

### Instructions:
1. **Extract Verbatim Data Only**: Extract information only if explicitly present in the raw text, tables, query results, or PDF. Do NOT infer, generate, or include fields not found.
2. **Omit Missing Fields**: If a field is not found, do NOT include it in the output. Do NOT use "Not Mentioned", "None", or empty strings.
3. **Field Names**: Use the exact field names provided below, preserving case and format.
4. **Data Formats**:
   - **Dates**: Normalize to `dd-MM-yyyy` (e.g., `16-06-2025`) or convert `MMM-yy` (e.g., `May-25`) to `01-MM-yy`. If invalid, omit the field.
   - **Numbers/Weights**: Extract as strings representing positive floats without units (e.g., `686.53` for 686.530 kg). If invalid, omit the field.
   - **Container Counts**: Extract as strings representing positive integers (e.g., `38`). If invalid, omit the field.
   - **Text Fields**: Extract as strings (e.g., `BF25001135`, `MYCOPHENOLATE MOFETIL`).
5. **Table Parsing**:
   - Prioritize table data for weights (`WMS Finished Product Gross Weight-N`, `Tare Weight-N`, `Net Weight-N`, `Current Gross Weight-N`), quantities, and `No of Container`.
   - Map table headers semantically (e.g., "Gross wt." to "WMS Finished Product Gross Weight-N").
   - Extract weights for containers 1 to 38 based on table rows or text patterns like `<Container No>. <Gross wt> <Tare wt> <Net wt> <Current Gross wt>`.
6. **Query Results**: Use Textract query results to supplement data, prioritizing matches for `Batch Number`, `Product Name`, etc.
7. **Validation**:
   - Validate weights as positive floats.
   - Validate dates as `dd-MM-yyyy` or `MMM-yy` (convert to `01-MM-yy`).
   - Omit invalid values from the output.
8. **Robustness**:
   - Handle handwritten text using query results and PDF content.
   - Cross-reference raw text, tables, queries, and PDF to resolve ambiguities.
   - Account for field name variations (e.g., "Batch No.", "Mfg Date").
   - Process multi-page PDFs and large tables (up to 38 containers).
9. **Output**: Return a JSON object with only the fields found, using the exact 153 field names listed below (excluding `File Name` and `S3 File Path`).

### Fields to Extract:
{', '.join(DETAILS_TO_EXTRACT[:-2])}

### Extracted Text, Tables, and Queries:
{textract_text}

### Output Format:
```json
{json_example}
"""
    try:
        pdf_response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        document_bytes = pdf_response["Body"].read()

        file_name = file_key.split('/')[-1]
        logger.info(f"Using original file name for Bedrock: {file_name}")

        conversation = [
            {
                "role": "user",
                "content": [
                    {"text": prompt},
                    {
                        "document": {
                            "format": "pdf",
                            "name": file_name,
                            "source": {"bytes": document_bytes}
                        }
                    }
                ]
            }
        ]

        for attempt in range(retries):
            try:
                response = bedrock_client.converse(
                    modelId=MODEL_NOVA_PRO,
                    messages=conversation,
                    inferenceConfig={
                        "maxTokens": 10000,
                        "temperature": 0.1,
                        "topP": 0.7
                    }
                )
                response_text = response["output"]["message"]["content"][0]["text"]
                logger.info(f"Bedrock raw output: {response_text[:1000]}...")

                json_match = re.search(r"\{.*\}", response_text, re.DOTALL)
                if not json_match:
                    logger.error("No valid JSON found in Bedrock response")
                    return extract_fields_from_text(textract_text, query_blocks)

                structured_data = json.loads(json_match.group(0))
                
                validated_data = {}
                for field in structured_data:
                    if field not in DETAILS_TO_EXTRACT[:-2]:
                        continue
                    validated_value = validate_field(field, structured_data[field])
                    if validated_value:
                        validated_data[field] = validated_value

                return validated_data

            except Exception as e:
                logger.error(f"Attempt {attempt + 1}/{retries} - Error invoking Amazon Nova Pro: {str(e)}")
                if attempt == retries - 1:
                    logger.warning("Bedrock failed after all retries, falling back to text extraction")
                    return extract_fields_from_text(textract_text, query_blocks)
                time.sleep(delay)

    except Exception as e:
        logger.error(f"Error preparing Bedrock request or retrieving PDF: {str(e)}")
        return extract_fields_from_text(textract_text, query_blocks)

def write_to_s3(df, retries=3, delay=5):
    """Writes DataFrame to an Excel file and uploads it to S3 with retries."""
    for attempt in range(retries):
        try:
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                df.to_excel(writer, index=False)
            s3_client.put_object(Bucket=OUTPUT_BUCKET, Key=OUTPUT_FILE_KEY, Body=excel_buffer.getvalue())
            logger.info(f"Successfully wrote to s3://{OUTPUT_BUCKET}/{OUTPUT_FILE_KEY}")
            return
        except Exception as e:
            logger.error(f"Attempt {attempt + 1}/{retries} - Error writing to S3: {str(e)}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise

def update_excel_from_dynamodb():
    """Aggregate all successful DynamoDB records into the Excel file."""
    try:
        records = get_all_dynamodb_records()
        data_list = []
        for record in records:
            status = record.get('ProcessingStatus', {}).get('S', '')
            if status != 'SUCCESS':
                continue
            structured_data = record.get('StructuredData', {}).get('M', {})
            s3_file_path = record.get('S3FilePath', {}).get('S', 'Unknown')
            if not isinstance(structured_data, dict):
                logger.warning(f"Skipping record for {s3_file_path} due to invalid StructuredData format")
                continue
            if not structured_data:
                logger.warning(f"Skipping record for {s3_file_path} due to empty StructuredData")
                continue
            row_data = {}
            for key in DETAILS_TO_EXTRACT:
                value = structured_data.get(key, {}).get('S', '')
                row_data[key] = value
            non_empty_values = sum(1 for value in row_data.values() if value != '')
            if non_empty_values > 10:
                data_list.append(row_data)
            else:
                logger.warning(f"Skipping record for {s3_file_path} due to insufficient data")
        if not data_list:
            logger.info("No valid successful records found in DynamoDB to write to Excel.")
            return
        df_updated = pd.DataFrame(data_list, columns=DETAILS_TO_EXTRACT)
        write_to_s3(df_updated)
        logger.info(f"Updated Excel with {len(data_list)} records from DynamoDB.")
    except Exception as e:
        logger.error(f"Error updating Excel from DynamoDB: {str(e)}")
        raise

def lambda_handler(event, context):
    """AWS Lambda handler for processing S3 document uploads with Textract and DynamoDB integration."""
    try:
        logger.info(f"Received event: {json.dumps(event, indent=2)[:1000]}...")
        record = event["Records"][0]
        bucket_name = record["s3"]["bucket"]["name"]
        file_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        s3_file_path = f"s3://{bucket_name}/{file_key}"
        file_name = file_key.split('/')[-1]
        logger.info(f"Processing file: {s3_file_path}")

        if not file_key.startswith(INPUT_PREFIX):
            logger.warning(f"File {s3_file_path} is not in {INPUT_PREFIX}, skipping.")
            return {"statusCode": 200, "body": json.dumps({"message": "File not in input prefix"})}

        if not file_name.lower().endswith('.pdf'):
            logger.error(f"File {s3_file_path} is not a PDF, skipping.")
            raise Exception(f"File {s3_file_path} is not a PDF")

        is_processed, error_message = check_dynamodb_record(s3_file_path)
        if is_processed:
            logger.info(f"File already processed with sufficient data: {s3_file_path}")
            update_excel_from_dynamodb()
            return {"statusCode": 200, "body": json.dumps({"message": "File already processed", "error": error_message})}

        textract_text, query_blocks = extract_text_and_tables_with_textract(bucket_name, file_key)
        structured_data = send_to_bedrock_model(textract_text, query_blocks, bucket_name, file_key)
        if not isinstance(structured_data, dict):
            logger.error(f"Structured data is not a dictionary: {structured_data}")
            structured_data = {field: "" for field in DETAILS_TO_EXTRACT}
            save_to_dynamodb(s3_file_path, file_name, structured_data, status="FAILED", error_message="Invalid structured data from Bedrock")
            raise Exception("Invalid structured data from Bedrock")

        structured_data["File Name"] = file_name
        structured_data["S3 File Path"] = s3_file_path

        full_structured_data = {field: structured_data.get(field, "") for field in DETAILS_TO_EXTRACT}
        
        non_empty_values = sum(1 for value in full_structured_data.values() if value != '')
        if non_empty_values < 10:
            logger.error(f"Extracted data for {s3_file_path} has insufficient valid fields: {non_empty_values}")
            save_to_dynamodb(s3_file_path, file_name, full_structured_data, status="FAILED", error_message=f"Insufficient valid fields extracted: {non_empty_values}")
            raise Exception(f"Insufficient valid fields extracted: {non_empty_values}")

        logger.info(f"Extracted data: {json.dumps(full_structured_data, indent=2)[:1000]}...")
        save_to_dynamodb(s3_file_path, file_name, full_structured_data, status="SUCCESS")
        update_excel_from_dynamodb()

        return {"statusCode": 200, "body": json.dumps({"message": "Text, tables, and queries processed successfully"})}

    except Exception as e:
        logger.error(f"Error processing file {s3_file_path}: {str(e)}")
        save_to_dynamodb(s3_file_path, file_name, {field: "" for field in DETAILS_TO_EXTRACT}, status="FAILED", error_message=str(e))
        return {"statusCode": 500, "body": json.dumps({"message": "Error processing text, tables, and queries", "error": str(e)})}