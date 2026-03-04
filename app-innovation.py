import streamlit as st
import boto3
import pandas as pd
import io
import json
import logging
from botocore.exceptions import ClientError
from functools import lru_cache
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# AWS Configuration
AWS_REGION = "us-east-1"
S3_BUCKET_NAME = "modernization-assessment"
S3_FILE_KEY = "Security_Questions_App_Infra.xlsx"
BEDROCK_MODEL_ID = "amazon.titan-text-premier-v1:0"
BLOG_LINK = "https://aws.amazon.com/blogs/migration-and-modernization/move-to-ai-pathway/"
def download_excel_file_from_s3():
    """Downloads the Excel file from the specified S3 bucket."""
    try:
        s3_client = boto3.client("s3", region_name=AWS_REGION)
        obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_FILE_KEY)
        file_content = io.BytesIO(obj["Body"].read())
        return pd.ExcelFile(file_content)
    except Exception as e:
        st.error(f"Error accessing S3 file: {e}")
        logger.error(f"S3 error: {e}")
        return None
@lru_cache(maxsize=32)
def fetch_questions_from_excel(excel_file):
    """Fetches questions from the specified Excel file."""
    modules = {}
    try:
        sheet_name = excel_file.sheet_names[0]
        df = excel_file.parse(sheet_name, header=None)
        questions_col = df.iloc[:, 1].dropna().tolist()
        current_module = None
        for question in questions_col:
            if any(keyword in question for keyword in ["Tech. Stack", "Security", "Code Quality", "Testing", "AWS Infra."]):
                current_module = question.split(':')[0].strip()
                modules[current_module] = []
            elif current_module:
                modules[current_module].append(question)
    except Exception as e:
        st.error(f"Error reading Excel file: {e}")
        logger.error(f"Excel parsing error: {e}")
        return {}
    return modules
def invoke_bedrock_model(input_text):
    """Invokes Amazon Bedrock Titan Text Premier model to generate recommendations."""
    try:
        bedrock_client = boto3.client("bedrock-runtime", region_name=AWS_REGION)
        payload = {
            "modelId": BEDROCK_MODEL_ID,
            "contentType": "application/json",
            "accept": "application/json",
            "body": json.dumps({
                "inputText": input_text,
                "textGenerationConfig": {
                    "maxTokenCount": 3072,
                    "stopSequences": [],
                    "temperature": 0.7,
                    "topP": 0.9
                }
            })
        }
        logger.info(f"Invoking Bedrock model with payload: {payload}")
        response = bedrock_client.invoke_model(**payload)
        response_body = json.loads(response["body"].read())
        logger.info(f"Bedrock response: {response_body}")
        if "results" in response_body and len(response_body["results"]) > 0:
            return response_body["results"][0].get("outputText", "No response from model.")
        else:
            logger.warning("No results found in Bedrock response.")
            st.error("Failed to get response from Bedrock model.")
            return "Unable to generate recommendation at this time."
    except ClientError as e:
        st.error(f"Error invoking Bedrock model: {e}")
        logger.error(f"Bedrock error: {e}")
        return "Unable to generate recommendation due to an error."
def generate_recommendations_batch(user_responses):
    """Generates recommendations for all questions in a single model invocation."""
    combined_input = " ".join(user_responses.values())
    prompt = f"""Analyze the following responses and generate detailed recommendations in the following format:
        1. AWS Service Recommendation: 
            - Compute
            - Database
            - Storage
            - CI/CD
            - Integration Services
        2. If already using AWS services, give best practices
        3. Security best practices
        4. Cost Optimization techniques
        Responses:
        {combined_input}
    """
    return invoke_bedrock_model(prompt)
def display_recommendations(summary, modules_with_questions, user_responses):
    """Displays a structured summary of recommendations."""
    st.subheader("Quadra Modernization Insights and Advice")
    if summary:
        st.markdown(summary)
    else:
        for module, questions in modules_with_questions.items():
            st.markdown(f"### {module} Recommendations")
            for question in questions:
                st.markdown(f"**Question: {question} - Your Response: {user_responses[question]}**")
                st.write("Generating recommendation...")  # Placeholder text
def answer_additional_question(question):
    """Generate an answer using an intelligent AI service."""
    # Here you should integrate with an intelligent service capable of Q&A
    # For demonstration, here's a placeholder
    try:
        # Hypothetically, calling an external AI service:
        response = invoke_bedrock_model(f"Answer this question: {question}")
        return response
    except Exception as e:
        logger.error(f"Error answering question: {e}")
        return "Unable to generate a response at this time."
def main():
    st.title("AWS Modernization Assessment Tool")
    st.header("Current Application Assessment Questions")
    excel_file = download_excel_file_from_s3()
    if not excel_file:
        return
    modules_with_questions = fetch_questions_from_excel(excel_file)
    if not modules_with_questions:
        st.error("No questions found in the Excel file.")
        return
    user_responses = {}
    for module, questions in modules_with_questions.items():
        st.markdown(f"### {module} Questions")
        for question in questions:
            response = st.text_area(f"{question}", key=question)
            user_responses[question] = response if response else "No response provided"
    all_filled = all(value.strip() != "" for value in user_responses.values())
    if all_filled:
        if st.button("Submit Responses"):
            st.success("Responses submitted successfully!")
            with st.spinner('Generating recommendations...'):
                summary = generate_recommendations_batch(user_responses)
            display_recommendations(summary, modules_with_questions, user_responses)
        
        # Additional questions section
        st.markdown("#### Do you have any additional questions?")
        additional_question = st.text_input("Enter your question here:")
        if additional_question:
            answer = answer_additional_question(additional_question)
            st.markdown(f"**Answer:** {answer}")
    else:
        st.info("Please fill in all responses to enable submission.")
if __name__ == "__main__":
    main()