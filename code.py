import json
import boto3
import logging
import time
import re
import csv
from io import StringIO
from botocore.exceptions import ClientError
from urllib.parse import unquote

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
textract_client = boto3.client('textract')
bedrock_client = boto3.client('bedrock-runtime')

# Nova Pro model ID
NOVA_PRO_MODEL_ID = "amazon.nova-pro-v1:0"

def lambda_handler(event, context):
    """
    Triggered when a PDF is uploaded to S3 (input folder).
    Extracts text with Textract, parses invoice fields using Nova Pro,
    saves structured JSON and CSV to S3 (output folder).
    PDF is NOT deleted - remains in input folder.
    """
    bucket_name = 'ampo0411'   # Destination bucket
    input_prefix = 'input/'
    output_prefix = 'output/'

    logger.info("Lambda started: event received")

    try:
        if 'Records' not in event or not event['Records']:
            raise ValueError("Invalid event structure — no Records found")

        source_bucket = event['Records'][0]['s3']['bucket']['name']
        raw_key = event['Records'][0]['s3']['object']['key']
        source_key = unquote(raw_key).replace("+", " ")
        logger.info(f"Processing file: {source_key} from bucket: {source_bucket}")

        if not source_key.startswith(input_prefix):
            logger.info(f"File {source_key} not in input folder, skipping")
            return {'statusCode': 200, 'body': 'Not in input folder'}

        # Step 1: Textract extraction
        full_text, _ = extract_text_textract(source_bucket, source_key)
        logger.info("Textract extraction complete, invoking Nova for field extraction")

        # Step 2: Extract structured invoice fields with Nova Pro
        extracted_fields = extract_invoice_fields_with_nova(full_text)
        logger.info(f"Extracted Fields: {json.dumps(extracted_fields, indent=2)}")

        # Get base filename without extension
        file_name = source_key.split('/')[-1].rsplit('.', 1)[0]
        
        # Step 3: Save structured JSON to S3
        json_key = f"{output_prefix}{file_name}.json"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=json_key,
            Body=json.dumps(extracted_fields, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
        logger.info(f"Saved JSON to {json_key}")

        # Step 4: Convert JSON to CSV and save
        csv_key = f"{output_prefix}{file_name}.csv"
        csv_content = convert_invoice_json_to_csv(extracted_fields)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=csv_key,
            Body=csv_content.encode('utf-8'),
            ContentType='text/csv'
        )
        logger.info(f"Saved CSV to {csv_key}")

        # PDF is NOT deleted - remains in input folder for reference
        logger.info(f"Processing complete. Original PDF retained at {source_key}")

        return {
            'statusCode': 200,
            'body': f"Invoice processed. JSON: {json_key}, CSV: {csv_key}, PDF retained: {source_key}"
        }

    except Exception as e:
        logger.error(f"Error in Lambda execution: {str(e)}")
        return {'statusCode': 500, 'body': str(e)}


def extract_text_textract(bucket, key):
    """Extract text from PDF using Amazon Textract asynchronous API."""
    try:
        job = textract_client.start_document_text_detection(
            DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}}
        )
        job_id = job["JobId"]
        logger.info(f"Started Textract Job: {job_id}")

        # Poll until job finishes
        while True:
            result = textract_client.get_document_text_detection(JobId=job_id)
            status = result["JobStatus"]
            if status == "SUCCEEDED":
                break
            elif status in ["FAILED", "PARTIAL_SUCCESS"]:
                raise Exception(f"Textract failed with status: {status}")
            time.sleep(5)

        text_blocks = []
        next_token = None
        while True:
            if next_token:
                result = textract_client.get_document_text_detection(JobId=job_id, NextToken=next_token)
            else:
                result = textract_client.get_document_text_detection(JobId=job_id)

            for block in result["Blocks"]:
                if block["BlockType"] == "LINE":
                    text_blocks.append(block["Text"])

            next_token = result.get("NextToken")
            if not next_token:
                break

        full_text = "\n".join(text_blocks)
        return full_text, text_blocks[0] if text_blocks else ""

    except Exception as e:
        logger.error(f"Textract error: {str(e)}")
        raise


def extract_invoice_fields_with_nova(full_text):
    """Use Nova Pro model to extract structured fields from invoice text."""
    try:
        system_prompt = """
        You are a precise invoice parser AI.
        Extract key invoice details in JSON format with exact keys:
        {
            "Invoice_No": "",
            "Invoice_Date": "",
            "Buyer_Name": "",
            "Seller_Name": "",
            "Buyer_Address": "",
            "Seller_Address": "",
            "GSTIN_Buyer": "",
            "GSTIN_Seller": "",
            "Total_Amount_Before_Tax": "",
            "CGST": "",
            "SGST": "",
            "IGST": "",
            "Total_Amount_After_Tax": "",
            "Items": [
                {
                    "Description": "",
                    "HSN_Code": "",
                    "Quantity": "",
                    "Rate": "",
                    "Amount": ""
                }
            ]
        }

        Use null for missing fields. Parse logically based on text context. Respond ONLY with the JSON object, no additional text.
        """

        payload = {
            "schemaVersion": "messages-v1",
            "messages": [
                {"role": "user", "content": [{"text": f"{system_prompt}\n\nInvoice Text:\n{full_text[:28000]}"}]}
            ],
            "inferenceConfig": {
                "maxTokens": 1500,
                "temperature": 0.3
            }
        }

        response = bedrock_client.invoke_model(
            modelId=NOVA_PRO_MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(payload)
        )

        # Parse response
        response_body = json.loads(response["body"].read().decode("utf-8"))
        
        ai_text = ""
        if "output" in response_body and "message" in response_body["output"]:
            message_content = response_body["output"]["message"].get("content", [])
            if message_content and isinstance(message_content, list) and len(message_content) > 0:
                if "text" in message_content[0]:
                    ai_text = message_content[0]["text"].strip()

        if not ai_text:
            raise ValueError("No text content found in Nova response")

        logger.info(f"Nova raw output: {ai_text}")

        # JSON extraction
        ai_text = re.sub(r'^``````$', '', ai_text, flags=re.MULTILINE)
        ai_text = re.sub(r'^``````$', '', ai_text, flags=re.MULTILINE)
        
        match = re.search(r'\{.*\}', ai_text, re.DOTALL)
        if match:
            json_str = match.group(0).strip()
            extracted_json = json.loads(json_str)
        else:
            extracted_json = json.loads(ai_text.strip())

        return extracted_json

    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error from Nova output: {str(e)}, raw: {ai_text}")
        return {"error": "Invalid JSON in model response", "raw_output": ai_text}
    except Exception as e:
        logger.error(f"Nova field extraction error: {str(e)}")
        return {"error": str(e)}


def convert_invoice_json_to_csv(invoice_data):
    """
    Convert invoice JSON to CSV format.
    Single combined table: Header fields + Item fields as columns.
    Each invoice line item becomes a row with repeated header values.
    """
    try:
        output = StringIO()
        writer = csv.writer(output)
        
        # ===== COMBINED SINGLE TABLE FORMAT =====
        # All columns: Invoice header fields + Item fields
        all_columns = [
            'Invoice_No',
            'Invoice_Date',
            'Buyer_Name',
            'Seller_Name',
            'Buyer_Address',
            'Seller_Address',
            'GSTIN_Buyer',
            'GSTIN_Seller',
            'Total_Amount_Before_Tax',
            'CGST',
            'SGST',
            'IGST',
            'Total_Amount_After_Tax',
            'Description',
            'HSN_Code',
            'Quantity',
            'Rate',
            'Amount'
        ]
        
        # Write header row
        writer.writerow(all_columns)
        
        # Extract header values
        header_values = [
            str(invoice_data.get('Invoice_No', '')),
            str(invoice_data.get('Invoice_Date', '')),
            str(invoice_data.get('Buyer_Name', '')),
            str(invoice_data.get('Seller_Name', '')),
            str(invoice_data.get('Buyer_Address', '')),
            str(invoice_data.get('Seller_Address', '')),
            str(invoice_data.get('GSTIN_Buyer', '')),
            str(invoice_data.get('GSTIN_Seller', '')),
            str(invoice_data.get('Total_Amount_Before_Tax', '')),
            str(invoice_data.get('CGST', '')),
            str(invoice_data.get('SGST', '')),
            str(invoice_data.get('IGST', '')),
            str(invoice_data.get('Total_Amount_After_Tax', ''))
        ]
        
        # Handle None values
        header_values = ['' if val == 'None' or val == 'null' else val for val in header_values]
        
        # Get items
        items = invoice_data.get('Items', [])
        
        if items and isinstance(items, list) and len(items) > 0:
            # Write one row per item, repeating header values
            for item in items:
                if isinstance(item, dict):
                    item_values = [
                        str(item.get('Description', '')),
                        str(item.get('HSN_Code', '')),
                        str(item.get('Quantity', '')),
                        str(item.get('Rate', '')),
                        str(item.get('Amount', ''))
                    ]
                    # Handle null values in items
                    item_values = ['' if val == 'None' or val == 'null' else val for val in item_values]
                    
                    # Combine header values + item values for this row
                    full_row = header_values + item_values
                    writer.writerow(full_row)
        else:
            # No items - write header values with empty item columns
            empty_items = ['', '', '', '', '']
            full_row = header_values + empty_items
            writer.writerow(full_row)
        
        csv_content = output.getvalue()
        output.close()
        
        return csv_content
        
    except Exception as e:
        logger.error(f"CSV conversion error: {str(e)}")
        return f"Error,{str(e)}\n"
