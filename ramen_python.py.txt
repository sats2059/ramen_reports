# -- Date: 5/27/2022
# -- About: This script has the various modules required for supporting Ramen reports
# -- !Important!
# Reusable modules:
# - Email function + HTML Formatting
# - Create Gsheet, Set Permissions and Extract to Gsheets
# - Execute SQL against Bigquery

# The meta data sheet for UI -> https://docs.google.com/spreadsheets/d/{gsheet_Id}
import sys

from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from google.cloud.exceptions import NotFound
from datetime import datetime
import pandas as pd
from googleapiclient import discovery
import smtplib
from email.message import EmailMessage
from email.mime.text import MIMEText
import os
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.utils import make_msgid
import json
from apiclient import discovery
from httplib2 import Http
from oauth2client import client, file, tools
from airflow.models import Variable


#Template to format html emails
BASE_TEMPLATE = """<html>
<head></head>
<body>{}</body>
</html>"""

def print_msg(msg, **kwargs):
    '''Logging function that accepts a string to print to terminal.'''
    today = datetime.now()
    print(f"{today}: {msg}")

def get_ramen_report_variables(**kwargs):
    '''Function used by Ramen reports to set environment variables for Dev and Prod'''
    VARS = Variable.get("ramen_report_params", deserialize_json=True)
    env_dict = {'project':VARS['project'], 'dataset':VARS['dataset'], 'sei_meta_model':VARS['sei_meta_model'],'meta_model_sheet_range':VARS['meta_model_sheet_range'],'ramen_reports_folder':VARS['ramen_reports_folder'], 'admin':VARS['admin']}
    return env_dict

def create_bq_client(cred_path=None, **kwargs):
    '''A sql execution routine that accepts credentials and SQL to be executed on Bigquery'''
    print_msg("create_bq_client")

    scopes = (
            'https://www.googleapis.com/auth/bigquery',
            'https://www.googleapis.com/auth/cloud-platform',
            'https://www.googleapis.com/auth/drive'
            )

    credentials = Credentials.from_service_account_file(cred_path)
    credentials = credentials.with_scopes(scopes)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return (client)

def submit_sql(cred_path=None, sql=None, **kwargs):
    '''A sql execution routine that accepts credentials and SQL to be executed on Bigquery'''
    print_msg("submit_sql")
    print_msg(f"SQL To be Executed: {sql}")
    client = create_bq_client(cred_path)
    client.query(sql).result()
    print("Query complete. Success!")

def big_query_connected_table_create(cred_path,project_id,dataset_id,gsheet_id,tab_name,sheet_range,table_id,schema,**kwargs):
    '''Create a Gsheet connected table using a bigquery API call. Variables to be passed: cred_path, big query project id, big query dataset, google sheet id, (Optional) tab within the sheet, the range of columns for the sheet, table name, schema (optional) for the table. '''

    client = create_bq_client(cred_path)
    dataset = client.get_dataset(f'{project_id}.{dataset_id}')

    table = bigquery.Table(dataset.table(table_id), schema=None)
    client.delete_table(table, not_found_ok=True)
    external_config = bigquery.ExternalConfig("GOOGLE_SHEETS")

    external_config.source_uris = [f"https://docs.google.com/spreadsheets/d/{gsheet_id}"]
    external_config.options.skip_leading_rows = 1
    external_config.options.range = sheet_range

    if tab_name is not None:
        external_config.options.range = (f"{tab_name}!{sheet_range}")

    if schema is None:
        external_config.autodetect = True
    else:
        external_config.schema = schema

    # if the value is `1` then it throws an error as in the table cell `C3` and `C5` has character.
    external_config.max_bad_records = 2

    external_config.ignore_unknown_values = True
    table.external_data_configuration = external_config

    table = client.create_table(table)
    print_msg(f"Table create:{table}")
    return table

def create_google_api_service_v4 (cred_path,**kwargs):
    scopes = (
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/drive.file',
        "https://www.googleapis.com/auth/spreadsheets"
    )

    credentials = Credentials.from_service_account_file(cred_path)
    credentials = credentials.with_scopes(scopes)
    service = discovery.build('sheets', 'v4', credentials=credentials)
    return (service)

def create_google_api_service_v3 (cred_path,**kwargs):
    scopes = (
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/drive.file',
        "https://www.googleapis.com/auth/spreadsheets"
    )

    credentials = Credentials.from_service_account_file(cred_path)
    credentials = credentials.with_scopes(scopes)
    service = discovery.build('drive', 'v3', credentials=credentials)
    return (service)

def create_google_form_service_v1(cred_path, **kwargs):
    scopes = (
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/drive.file',
        "https://www.googleapis.com/auth/spreadsheets"
    )

    credentials = Credentials.from_service_account_file(cred_path)
    credentials = credentials.with_scopes(scopes)
    forms_service = discovery.build("forms", 'v1', credentials=credentials)
    return forms_service

def gsheet_create(cred_path,title,**kwargs):
    '''This routine creates a new google sheet. Accepts credentials and string for a gsheet title and returns sheetid'''

    print_msg("gsheet_create")
    service = create_google_api_service_v4(cred_path)
    spreadsheet = {
        'properties': {
            'title': title
        }

    }
    spreadsheet = service.spreadsheets().create(body=spreadsheet,fields='spreadsheetId').execute()
    service = create_google_api_service_v3(cred_path)
    service.files().update(fileId=spreadsheet.get('spreadsheetId'))

    print_msg(f"Spreadsheet ID: {(spreadsheet.get('spreadsheetId'))}")
    return spreadsheet.get('spreadsheetId')

def drive_object_move(cred_path,file_id,dest_folder,**kwargs):

    print_msg(f"Moving Drive Object:{file_id} to {dest_folder}")
    service = create_google_api_service_v3(cred_path)
    service.files().update(fileId=file_id,addParents=dest_folder,fields='id, parents').execute()


def drive_object_set_perm(cred_path,file_id,emails,**kwargs):
    '''This routine sets write permission for a given google sheet to an email id. Accepts credentials, gsheet file id and an email id'''
    print_msg("drive_object_set_perm")
    service = create_google_api_service_v3(cred_path)

    permission1 = {
        'type': 'user',
        'role': 'writer',
        'emailAddress': emails,
        }
    req = service.permissions().create(fileId=file_id, body=permission1,sendNotificationEmail= False)
    req.execute()
    return None

def gsheet_clear(cred_path,sheetid, tab):
    '''This routine will clear all cells for a given google sheet. Accepts credentials and a google sheet id as string'''
    print_msg("gsheet_clear")
    service = create_google_api_service_v4(cred_path)

    resultlive = service.spreadsheets().values().get(spreadsheetId=sheetid, range=tab).execute()
    extractsheetread = pd.DataFrame(resultlive.get('values', []))

    # Getting the length from the sheet(tab) read above
    start = 1
    end = len(extractsheetread)

    # Only clearing/truncating the sheet if its not empty already.
    # This part clears data from cells instead of deleting them to ensure existing named ranges dont break
    if end >= 1:
        rangeAll = f"{tab}!1:{end}"
        body = {}
        service.spreadsheets().values().clear(spreadsheetId=sheetid, range=rangeAll, body=body).execute()

def gsheet_write(cred_path,sheetid, column_header, data, tab):
    print_msg("gsheet_write")

    service = create_google_api_service_v4(cred_path)

    #Null handelling for front end display
    data.fillna('')
    for col in data.columns:
        data[col] = data[col].astype(str)
        data[col] = data[col].apply(lambda x: x.replace('None','') if (len(x)==4 and x=='None') else x)
        data[col] = data[col].apply(lambda x: x.replace('nan','') if (len(x)==3 and x=='nan') else x)
    ## Converting to G-Sheet writable format
    df = []
    df.append(column_header)
    for rec in range(len(data.index)):
        df.append(list(data.iloc[rec]))
    # Returning data in the G-Sheet write format and writing to g-sheet
    gsheetwrite= {
    'values': df
    }

    #Writing to G-Sheet
    service.spreadsheets().values().append(spreadsheetId=sheetid, body=gsheetwrite, range=tab, valueInputOption='USER_ENTERED').execute()
    return "Complete"

def gsheet_get_data(cred_path,spreadsheet_id, project,dataset, etl_table,tab=None):
    '''This routine reads a given big query table as input and extracts the rows to a google sheet. Accepts credentials, google sheet id and big query table name and tab_name'''

    print_msg("gsheet_get_data")

    client = create_bq_client(cred_path)

    qualified_table_name = f"{project}.{dataset}.{etl_table}"

    sql = f"select * from `{qualified_table_name}` limit 2000"
    print_msg(sql)

    extractval = client.query(sql).result()
    print_msg("SQL Complete.")
    data = pd.DataFrame.from_records(list(extractval)).astype(str)


    # Getting the table header/column name list from the table
    tableDetail = client.get_table(qualified_table_name)
    name = [schema.name for schema in tableDetail.schema]
    header = []
    for pos, val in enumerate(name):
        header.append(val)

    # Writing/Appending to the g-sheet
    # Defaults to sheet1
    if tab == None:
        tab = 'sheet1'
    gsheet_write (cred_path,spreadsheet_id, header, data, tab)

def csv_to_list(csv_text):
    print_msg("csv_to_list")
    list = csv_text.split(",")
    return list

def default_body(html):
    """return default html body of email"""
    html = BASE_TEMPLATE.format(html)
    return html

def send_email(msg,**kwargs):
    '''This routine sends emails. Accepts MimeText build messages. Refer build_email_html to build message.'''
    s = smtplib.SMTP("smtp.corp.wayfair.com:25")
    s.send_message(msg)
    print("Email Success!")

def build_email_html(email_subject,email_from,email_to,email_body):
    '''This routine accepts email subject, from, to and email body and returns MIMEText as HTML. Send message using send_email function.'''
    print_msg("build_email_html")
    msg = EmailMessage()
    msg["Subject"] = email_subject
    msg["From"] = email_from
    msg["To"] = email_to
    body = MIMEText(email_body, 'html')
    msg.set_content(body)
    return(msg)

def get_form_results(cred_path,form_id,**kwargs):
    forms_service = create_google_form_service_v1(cred_path)
    details = forms_service.forms().get(formId=form_id).execute()
    return details

def gform_create(cred_path,project_name):
    form = {
        "info": {
            "title": f"Exclusion Form: {project_name}",
            "document_title": f"Ramen Report Exclusion form: {project_name}"
        },
    }
    # Prints the details of the sample form
    forms_service=create_google_form_service_v1(cred_path)
    result = forms_service.forms().create(body=form).execute()
    form_id = result.get('formId')
    return form_id

def ramen_report_create_folder(folder_name, cred_path,**kwargs):
    env=get_ramen_report_variables()
    service = create_google_api_service_v3(cred_path)
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    results = service.files().create(body=file_metadata,fields='id').execute()
    file_id=results.get('id')
    drive_object_set_perm(cred_path,file_id,[env['admin']])
    print(f"https://drive.google.com/drive/u/0/folders/{file_id}")

def ramen_report_meta_table(cred_path,**kwargs):
    '''Creates the meta data table for Ramen Reports on initial run'''
    print_msg("ramen_report_meta_table")
    client = create_bq_client(cred_path)
    env = get_ramen_report_variables()
    ddl = f"""CREATE TABLE if not exists `{env['project']}.{env['dataset']}.meta_ramen_reports`(
              dag_id INT64,
              product_id INT64,
              project_id INT64,
              gsheet_id STRING,
              gform_id STRING,
              gform_sheetid STRING
            );"""

    client.query(ddl).result()

def ramen_report_clear_meta_data (cred_path, dag_id,project_id,**kwargs):
    #Only works for manual overrides

    print_msg("ramen_report_meta_insert")
    env=get_ramen_report_variables()
    client = create_bq_client(cred_path)
    #Delete all entries for the project and dag in the meta_ramen_reports
    sql = f"Delete from `{env['project']}.{env['dataset']}.meta_ramen_reports` where dag_id = {dag_id} and project_id = {project_id};"
    client.query(sql).result()

def ramen_report_meta_update_gsheet (cred_path, dag_id,project_id,gsheet_id, **kwargs):
    print_msg("ramen_report_meta_insert")

    env=get_ramen_report_variables()
    client = create_bq_client(cred_path)
    #Update the entry for the project and dag with the gsheet id
    sql = f"Update `{env['project']}.{env['dataset']}.meta_ramen_reports` set gsheet_id = '{gsheet_id}' where dag_id = {dag_id} and project_id = {project_id};"
    client.query(sql).result()

def ramen_report_meta_update_gform (cred_path, dag_id,project_id,gformid, **kwargs):
    print_msg("ramen_report_meta_update_gform")
    env = get_ramen_report_variables()
    client = create_bq_client(cred_path)
    ramen_report_meta_table(cred_path)
    sql = f"Update `{env['project']}.{env['dataset']}.meta_ramen_reports` set gform_id = '{gformid}' where dag_id = {dag_id} and project_id = {project_id};"
    client.query(sql).result()

def ramen_report_meta_update_gform_linkedid (cred_path, dag_id,project_id,linked_sheet_id, **kwargs):
    print_msg("ramen_report_meta_update_gform_linkedid")
    env = get_ramen_report_variables()
    client = create_bq_client(cred_path)
    ramen_report_meta_table(cred_path)
    sql = f"Update `{env['project']}.{env['dataset']}.meta_ramen_reports` set gform_sheetid = '{linked_sheet_id}' where dag_id = {dag_id} and project_id = {project_id};"
    print_msg(sql)
    client.query(sql).result()

def ramen_report_gsheet_extract(dag_id,project_id,cred_path,**kwargs):

    print_msg("ramen_report_gsheet_extract")
    env = get_ramen_report_variables()
    client = create_bq_client(cred_path)


# Get the email list and the etl table name to extract to the gsheet
# If gsheet does not exist, it will create a new one

    sql = f"select email_list,etl_table_name,project_name from `{env['project']}.{env['dataset']}.meta_task` where dag_id = {dag_id} and project_id = {project_id}"
    results = client.query(sql).result()
    if results.total_rows == 0:
        print_msg("No Results")
        return
    if results.total_rows > 1:  ## exactly 1 row necessarily
        msg = f"Expected one row in `{env['project']}.{env['dataset']}.meta_task` for each project-DAG combination, received {results.total_rows} rows for dag_id = {dag_id} and project_id = {project_id}"
        raise ValueError(msg)
    for rows in results:
        email_text = rows.email_list
        etl_table_name = rows.etl_table_name
        project_name = rows.project_name
    email_list = csv_to_list(email_text)
    sql = f"select gsheet_id from `{env['project']}.{env['dataset']}.meta_ramen_reports` where dag_id = {dag_id} and project_id = {project_id}"
    print_msg(sql)
    results = client.query(sql).result()
    if results.total_rows > 1:  ## exactly 1 row necessarily
        msg = f"Expected one row in `{env['project']}.{env['dataset']}.meta_ramen_reports` for each project-DAG combination, received {results.total_rows} rows for dag_id = {dag_id} and project_id = {project_id}"
        raise ValueError(msg)
    for rows in results:
        gsheet_id = rows.gsheet_id

    if gsheet_id == None:
        # Create new gsheet as this metadata does not exists
        gsheet_id = gsheet_create(cred_path, f"Ramen Report: {project_name}")

        # Move the Gsheet to the ramen reports shared drive
        ramen_reports_folder = env['ramen_reports_folder']
        drive_object_move(cred_path, gsheet_id,ramen_reports_folder)

        #Insert the metadata into the ramen report meta tables for the new gsheet created
        ramen_report_meta_update_gsheet(cred_path,dag_id,project_id,gsheet_id)

        print('send email here to notify new report created with the Gsheet link below')
        print(f"https://docs.google.com/spreadsheets/d/{gsheet_id}")

    for emails in email_list:
        drive_object_set_perm(cred_path, gsheet_id, emails)
    gsheet_clear(cred_path,gsheet_id, 'sheet1')
    gsheet_get_data(cred_path,gsheet_id,env['project'],env['dataset'],etl_table_name)

def ramen_reports_send_email(dag_id,project_id,cred_path,**kwargs):
    print_msg("build_email_ramen_reports")
    env = get_ramen_report_variables()

    client = create_bq_client(cred_path)

    # Get the email list and the etl table name to extract to the gsheet
    # If gsheet does not exist, it will create a new one

    sql = f"select email_list,project_name from `{env['project']}.{env['dataset']}.meta_task` where dag_id = {dag_id} and project_id = {project_id}"
    results = client.query(sql).result()

    if results.total_rows == 0:
        print_msg("No Results")
        return
    if results.total_rows > 1:  ## exactly 1 row necessarily
        msg = f"Expected one row in `{env['project']}.{env['dataset']}.meta_task` for each project-DAG combination, received {results.total_rows} rows for dag_id = {dag_id} and project_id = {project_id}"
        raise ValueError(msg)
    for rows in results:
        email_text = rows.email_list
        project_name = rows.project_name

    email_list = csv_to_list(email_text)
    sql = f"select gsheet_id, gform_id from `{env['project']}.{env['dataset']}.meta_ramen_reports` where dag_id = {dag_id} and project_id = {project_id}"
    results = client.query(sql).result()

    if results.total_rows == 0:
        print_msg(f"No Entry in the meta_ramen_reports for project_id = {project_id} and dag_id = {dag_id}.")
        return
    else:
        if results.total_rows > 1:  ## exactly 1 row necessarily
            msg = f"Expected one row in `{env['project']}.{env['dataset']}.meta_ramen_reports` for each project-DAG combination, received {results.total_rows} rows for dag_id = {dag_id} and project_id = {project_id}"
            raise ValueError(msg)
        for rows in results:
            gsheet_id = rows.gsheet_id
            gform_id = rows.gform_id

        gsheet_url = f"https://docs.google.com/spreadsheets/d/{gsheet_id}"
        gform_url = f"https://docs.google.com/forms/d/{gform_id}"

        email_subject = f"Ramen Report refresh update: {project_name}"
        email_from = "spasupathy@wayfair.com"
        email_to = email_list
        body = f"Hello,<br><br> Your Ramen Report for Project {project_name} has been updated. Below is the link to the updated tracker and the exclusion form.<br><br><a size='1' href='{gsheet_url}'> Tracker sheet</a><br><br><a size='1' href='{gform_url}'>Exclusion Form</a><br><br>Regards,<br>FP SEI team"
        email_body = default_body(body)
        msg = build_email_html(email_subject,email_from,email_to,email_body)
        send_email(msg)


def ramen_report_etl(dag_id,project_id,cred_path,**kwargs):
    '''This Routine is used for executing SQL for the Ramen Reports'''
    print_msg("ramen_report_etl")
    env = get_ramen_report_variables()
    client = create_bq_client(cred_path)
    meta_sql = f"select t.etl_sql, t.etl_table_name, r.gform_sheetid, t.grain_to_exclude from `{env['project']}.{env['dataset']}.meta_task` t " \
          f" left join `{env['project']}.{env['dataset']}.meta_ramen_reports` r" \
          f" on t.dag_id = r.dag_id" \
          f" and t.project_id = r.project_id" \
          f" where t.dag_id = {dag_id} and t.project_id = {project_id}"

    print_msg(meta_sql)

    meta_vals = client.query(meta_sql).result()
    for rows in meta_vals:
        sql = rows.etl_sql
        gform_sheetid = rows.gform_sheetid
        grain_to_exclude = rows.grain_to_exclude
        etl_table_name = rows.etl_table_name

    #Setting all table name variables
    gform_results_table = f"{etl_table_name}_exclusion_table"
    snapshot_table = f"{etl_table_name}_snapshot"
    etl_sql = f"create or replace table `{env['project']}.{env['dataset']}.{etl_table_name}` as {sql}"


    snapshot_sql = f"CREATE TABLE IF NOT EXISTS `{env['project']}.{env['dataset']}.{snapshot_table}` as select *,current_datetime() as meta_snapshot_date from `{env['project']}.{env['dataset']}.{etl_table_name}` where 1 = 2;" \
                   f"insert into `{env['project']}.{env['dataset']}.{snapshot_table}` select *,current_datetime() as meta_snapshot_date from `{env['project']}.{env['dataset']}.{etl_table_name}`;"
    if gform_sheetid == None:
        print_msg('WARNING: The Linked Excel sheet for this Ramen Report has not been enabled. Items will not get excluded from the report.')

        submit_sql(cred_path,etl_sql)
        submit_sql(cred_path, snapshot_sql)
    else:
        # Append the exclude where clause to exclude all rows entered in through the google form
        exclude_sql = f" and cast({grain_to_exclude} as STRING) not in (select exclusion_grain from {env['project']}.{env['dataset']}.{gform_results_table})"
        etl_sql = etl_sql + exclude_sql

        submit_sql(cred_path, etl_sql)
        submit_sql(cred_path, snapshot_sql)
    return

def ramen_report_form_create(dag_id,project_id,cred_path,**kwargs):
    print_msg("ramen_report_form_create")
    env = get_ramen_report_variables()
    client = create_bq_client(cred_path)

    sql = f"select email_list,project_name,gform_options,etl_table_name from `{env['project']}.{env['dataset']}.meta_task` where dag_id = {dag_id} and project_id = {project_id}"
    print_msg(sql)
    results = client.query(sql).result()

    if results.total_rows == 0:
        print_msg(f"No Results for below sql: {sql}")
        return
    if results.total_rows > 1:  ## exactly 1 row necessarily
        msg = f"Expected one row in `{env['project']}.{env['dataset']}.meta_task` for each project-DAG combination, received {results.total_rows} rows for dag_id = {dag_id} and project_id = {project_id}"
        raise ValueError(msg)
    for rows in results:
        email_text = rows.email_list
        project_name = rows.project_name
        form_options_text = rows.gform_options
        etl_table_name = rows.etl_table_name

    gform_results_table = f"{etl_table_name}_exclusion_table"
    email_list = csv_to_list(email_text)
    form_options = csv_to_list(form_options_text)

    # Get the ramen report form id if exists
    sql = f"select gform_id from `{env['project']}.{env['dataset']}.meta_ramen_reports` where dag_id = {dag_id} and project_id = {project_id}"
    results = client.query(sql).result()
    print_msg(sql)
    if results.total_rows > 1:  ## exactly 1 row necessarily
        msg = f"Expected one row in `{env['project']}.{env['dataset']}.meta_ramen_reports` for each project-DAG combination, received {results.total_rows} rows for dag_id = {dag_id} and project_id = {project_id}"
        raise ValueError(msg)
    for rows in results:
        gform_id = rows.gform_id


    if gform_id == None:
        if len(form_options) != 4:
            msg = f"""Invalid number of options provided in sei_meta_model for dag_id = {dag_id} and project_id = {project_id}. gform_options on sei_meta_model must have exactly 4 comma separated values. Ex: Option 1, Option 2, Option 3, Option 4. The form for this project will not be created."""
            raise ValueError(msg)

        else:
            # Create new gform as this metadata does not exists
            gform_id = gform_create(cred_path, f"{project_name}")

            # Move the gsheet to the ramen reports shared drive
            ramen_reports_folder = env['ramen_reports_folder']
            drive_object_move(cred_path, gform_id,ramen_reports_folder)

            # Insert the metadata into the ramen report meta tables for the new gsheet created
            ramen_report_meta_update_gform(cred_path, dag_id, project_id, gform_id)

            update = {
                "requests": [{"createItem": {
                    "item": {
                        "title": "Enter The ID to exclude",
                        "questionItem": {
                            "question": {
                                "required": True,
                                "textQuestion": {
                                    "paragraph": False
                                }
                            }
                        }
                    },
                    "location": {
                        "index": 0
                    }
                }},
                    {"createItem": {
                        "item": {
                            "title": "Reason for Exclusion",
                            "questionItem": {
                                "question": {
                                    "required": True,
                                    "choiceQuestion": {
                                        "type": "DROP_DOWN",
                                        "options": [
                                            {"value": f"{form_options[0]}"},
                                            {"value": f"{form_options[1]}"},
                                            {"value": f"{form_options[2]}"},
                                            {"value": f"{form_options[3]}"}
                                        ]
                                    }
                                }
                            }
                        },
                        "location": {
                            "index": 1
                        }
                    }}
                ]
            }
            form_service = create_google_form_service_v1(cred_path)
            form_service.forms().batchUpdate(formId=gform_id, body=update).execute()


    #set permissions for the form
    for emails in email_list:
        drive_object_set_perm(cred_path, gform_id, emails)

    #Move the form to the ramen reports shared drive
    ramen_reports_folder = env['ramen_reports_folder']
    drive_object_move(cred_path,gform_id,ramen_reports_folder)

    #Check if the form has a connected sheet
    results = get_form_results(cred_path, gform_id)
    print_msg(results.get('linkedSheetId'))


    if results.get('linkedSheetId') == None:
        print_msg('LinkedSheetId is None. Send Email to Admin to link sheet.')
        email_subject = f"Ramen Report: Project id: {project_id}"
        email_from = env['admin']
        email_to = env['admin']
        body = f"Hello,<br><br> Please enable the Linked Sheet option on the Exclusion form for the below Ramen Report; " \
               f"<br><br> Project_Id: {project_id}, and Dag_id: {dag_id}" \
               f"<br><br> https://docs.google.com/forms/d/{gform_id}/edit" \
               f"<br><br>Regards,<br>FP SEI team"
        email_body = default_body(body)
        msg = build_email_html(email_subject, email_from, email_to, email_body)
        send_email(msg)
    else:
        linkedSheetId = results.get('linkedSheetId')
        ramen_report_meta_update_gform_linkedid(cred_path,dag_id,project_id,linkedSheetId)
        # Hard coded schema for 3 fields
        schema = [
            bigquery.SchemaField("Timestamp", "TIMESTAMP"),
            bigquery.SchemaField("exclusion_grain", "STRING"),
            bigquery.SchemaField("exclusion_reason", "STRING")
        ]
        big_query_connected_table_create(cred_path=cred_path,project_id=env['project'],dataset_id=env['dataset'],gsheet_id=linkedSheetId,tab_name=None,sheet_range='A:C',table_id=gform_results_table,schema=schema)

def ramen_report_meta_validate(dag_id,project_id, cred_path,**kwargs):
    #Get the environment variable
    print_msg('ramen_report_meta_validate')
    env=get_ramen_report_variables()
    client = create_bq_client(cred_path)

    # Validate the meta_task table exists, if not create the meta table in the env using parms in the env dict
    try:
        client.get_table(f"{env['project']}.{env['dataset']}.meta_task")  # Make an API request.
        print_msg(f"Table exists: {env['project']}.{env['dataset']}.meta_task")
    except NotFound:
        print_msg(f"Table not found: {env['project']}.{env['dataset']}.meta_task; Executing the DDL.")
        big_query_connected_table_create(cred_path, env['project'], env['dataset'],env['sei_meta_model'], 'MetaTask',env['meta_model_sheet_range'], 'meta_task',None)

    #Validate the meta entry exists in the sei_meta_model
    sql = f"select * from `{env['project']}.{env['dataset']}.meta_task` where dag_id = {dag_id} and project_id = {project_id}"
    print_msg(sql)
    results = client.query(sql).result()

    if results.total_rows == 0:
        print_msg('No Meta Entries found')
        email_subject = f"Ramen Report validate:WARN: project id: {project_id}"
        email_from = env['admin']
        email_to = env['admin']
        body = f"Hello,<br><br> Warning: Ramen Report for Project_Id: {project_id}, and Dag_id: {dag_id} is missing Metadata information in {env['project']}.{env['dataset']}.meta_task connected table, linked to " \
               f"the https://docs.google.com/spreadsheets/d/{env['sei_meta_model']}<br><br>Regards,<br>FP SEI team"
        email_body = default_body(body)
        msg = build_email_html(email_subject, email_from, email_to, email_body)
        send_email(msg)
        sys.exit(1)

    # Create the meta_ramen_reports table if it does not exist in the env
    ramen_report_meta_table(cred_path)

    # Insert row for the project and dag in meta_ramen_reports table if it does not exist in the env
    sql = f"select * from `{env['project']}.{env['dataset']}.meta_ramen_reports` where dag_id = {dag_id} and project_id = {project_id}"
    print_msg(sql)
    result = client.query(sql).result()

    if result.total_rows == 0:
        print_msg('WARNING: Missing meta entry in the meta_ramen_reports. Inserting row for the project.')
        sql = f"Insert into `{env['project']}.{env['dataset']}.meta_ramen_reports`(dag_id,project_id) values({dag_id},{project_id});"
        print_msg(sql)
        client.query(sql).result()
