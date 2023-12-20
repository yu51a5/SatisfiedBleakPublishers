import os

from google.cloud import bigquery
from google.oauth2 import service_account

queries = [   
          """SELECT quarter FROM `fh-bigquery.stackoverflow_archive_questions.merged` ORDER BY quarter DESC LIMIT 1""",
          """SELECT creation_date FROM `fh-bigquery.stackoverflow_archive_questions.merged` ORDER BY creation_date DESC LIMIT 1"""
]

#The purpose of this basic project is to  select the shortest StackOverflow questions containing "hello" and "world".

# Authentication: create a service account, generate a key in JSON format 
# Next save its contents (everything except opening `{` and closing `}`) as google_key secret.

# how-to: https://docs.aws.amazon.com/dms/latest/sbs/bigquery-redshift-migration-step-1.html (steps 1.-10. on one web page)
# thanks: https://stackoverflow.com/questions/73195754/python-google-bigquery-how-to-authenticate-without-json-file
cred = service_account.Credentials.from_service_account_info(eval("{" + os.environ['google_key'] +"}"))
client = bigquery.Client(credentials=cred, project=cred.project_id)

for i, q in enumerate(queries):

# SQL dialect reference: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
  query_job = client.query(q)
  results = query_job.result()  # Waits for job to complete.
  print(f"\nResult of query\n''{q}''\nis:")
  for row in results:
    print(row)

print("\nall done!")
