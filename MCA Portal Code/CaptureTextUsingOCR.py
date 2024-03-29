import boto3
import time


def extract_text_from_pdf(pdf_file_path, bucket_name, object_name,config_dict_ocr):
    # Upload the PDF file to S3
    access_key = config_dict_ocr['aws_access_key']
    secret_access_key = config_dict_ocr['aws_secret_access_key']
    s3_client = boto3.client('s3', region_name='ap-south-1', aws_access_key_id=access_key,aws_secret_access_key=secret_access_key)

    s3_client.upload_file(pdf_file_path, bucket_name, object_name)

    # Initialize the Textract client
    textract_client = boto3.client('textract',region_name='ap-south-1', aws_access_key_id=access_key,aws_secret_access_key=secret_access_key)

    # Start the Textract analysis
    response = textract_client.start_document_text_detection(
        DocumentLocation={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': object_name
            }
        }
    )

    # Get the JobId from the response
    job_id = response['JobId']

    # Poll the Textract job status
    while True:
        result = textract_client.get_document_text_detection(JobId=job_id)
        status = result['JobStatus']

        if status in ['SUCCEEDED', 'FAILED']:
            break

        print(f"Job status: {status}")
        time.sleep(5)  # Adjust the polling interval as needed

    # Check if the job was successful
    if status == 'SUCCEEDED':
        # Extract and return the text
        extracted_text = ''
        for item in result['Blocks']:
            if item['BlockType'] == 'LINE':
                extracted_text += item['Text'] + '\n'
        return extracted_text.strip()
    else:
        # Handle the case where the job failed
        print(f"Textract job failed with status: {status}")
        return None