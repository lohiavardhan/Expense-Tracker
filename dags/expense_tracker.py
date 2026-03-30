# from airflow import DAG
# from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pickle
import os.path
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import json
import base64
import re
from bs4 import BeautifulSoup


SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

default_args = {
    'owner': 'vardhan',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_gmail_service():
    creds = None
    if os.path.exists('../token.json'):
        creds = Credentials.from_authorized_user_file('../token.json', SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open('../token.json', 'w') as token:
                token.write(creds.to_json())
        else:
            raise Exception("No valid token. Run auth locally first and copy token.json to server.")

    return build('gmail', 'v1', credentials=creds)

def fetch_emails():
    """Pull all emails from Gmail API"""

    service = get_gmail_service()
    results = service.users().messages().list(userId='me', q='newer_than:12h').execute()
    messages = results.get('messages', [])
    banking_emails = []
    all_email = []

    for i, msg in enumerate(messages):
        detail = service.users().messages().get(userId='me', id=msg['id']).execute()
        for payload_dict in detail['payload']['headers']:
            if payload_dict['name'] == "From":
                if payload_dict['value'] == "\"ibanking.alert@dbs.com\" <ibanking.alert@dbs.com>":
                    banking_emails.append(detail)
        
        all_email.append(detail)
    
    return banking_emails, all_email

def get_email_body(payload):
    if 'data' in payload.get('body', {}):
        return payload['body']['data']
    if 'parts' in payload:
        for part in payload['parts']:
            if part['mimeType'] == 'text/html':
                return part['body']['data']
    return None

def get_header(headers, name):
    for h in headers:
        if h['name'] == name:
            return h['value']
    return None

def parse_emails(banking, all_emails):
    transactions = []
    
    for detail in banking:
        headers = detail['payload']['headers']
        
        data = get_email_body(detail['payload'])
        if not data:
            continue
        
        html_content = base64.urlsafe_b64decode(data).decode('utf-8')
        text = BeautifulSoup(html_content, 'html.parser').get_text()
        
        # Determine type
        subject = get_header(headers, 'Subject') or ''
        if 'Card' in subject:
            txn_type = 'Card'
        elif 'PayNow' in subject or 'iBanking' in subject:
            txn_type = 'PayNow'
        else:
            txn_type = 'Unknown'
        
        # Extract fields from email body
        amount = re.search(r'Amount:\s*(SGD[\d,.]+)', text)
        date = re.search(r'Date & Time:\s*(.+?)(?:\s{2,}|\n)', text)
        to_merchant = re.search(r'To:\s*(.+?)(?:\s*\(UEN|\s*If\s|(?:\s{2,}|\n))', text)
        from_card = re.search(r'From:\s*(.+?)(?:\s{2,}|\n)', text)
        
        txn = {
            'email_id': detail['id'],
            'date': date.group(1).strip() if date else None,
            'from': from_card.group(1).strip() if from_card else None,
            'to': to_merchant.group(1).strip() if to_merchant else None,
            'subject': subject,
            'amount': amount.group(1).strip() if amount else None,
            'type': txn_type,
        }
        
        transactions.append(txn)
    
    print(transactions)
    return transactions

        

def categorize(**context):
    """Assign category based on merchant name"""
    # TODO: Keyword matching logic
    print("Categorizing expenses...")

def load_to_db(**context):
    """Insert cleaned transactions into PostgreSQL"""
    # TODO: Database insert
    print("Loading to database...")

banking, all_txns = fetch_emails()
parse_emails(banking, all_txns)

# with DAG(
#     dag_id='expense_tracker',
#     default_args=default_args,
#     description='Track expenses from bank emails',
#     schedule_interval='@hourly',
#     start_date=datetime(2026, 3, 29),
#     catchup=False,
#     tags=['expenses'],
# ) as dag:

#     t1 = PythonOperator(task_id='fetch_emails', python_callable=fetch_emails)
#     t2 = PythonOperator(task_id='parse_transactions', python_callable=parse_transactions)
#     t3 = PythonOperator(task_id='categorize', python_callable=categorize)
#     t4 = PythonOperator(task_id='load_to_db', python_callable=load_to_db)

#     t1 >> t2 >> t3 >> t4
