import apache_beam as beam
from apache_beam.dataframe.convert import to_dataframe
import pandas as pd
import typing
import os


# setup paths
inp_path = 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'

curdir = os.path.dirname(__file__)
outdir = curdir + r"\output"

task1_out = outdir + r"\output_task1"
task2_out = outdir + r"\output_task2"
task3_out = outdir + r"\output_task3"
task4_out = outdir + r"\output_task4"
print(task1_out)

class Transaction(typing.NamedTuple):
    timestamp: str
    origin: str
    destination: str
    transaction_amount: float

# task 1
with beam.Pipeline() as pipeline:
    agg = (
        pipeline
        | beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv', skip_header_lines=1)
        | beam.Map(lambda line: line.split(','))
        | beam.Filter(lambda x: float(x[3]) >= 20)
        | beam.Map(lambda x: f'{x[0][:10]},{float(x[3])}')
        | beam.io.WriteToText(task1_out, 
                              file_name_suffix='.csv',
                              header='timestamp,transaction_amount')
        )

# task 2
with beam.Pipeline() as pipeline:
    agg = (
        pipeline
        | beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv', skip_header_lines=1)
        | beam.Map(lambda line: line.split(','))
        | beam.Filter(lambda x: x[0][:10] >= '2010-01-01')
        | beam.Map(lambda x: f'{x[0][:10]},{float(x[3])}')
        | beam.io.WriteToText(task2_out, 
                              file_name_suffix='.csv',
                              header='timestamp,transaction_amount')
        )

# task 3
with beam.Pipeline() as pipeline:
    agg = (
        pipeline
        | beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv', skip_header_lines=1)
        | beam.Map(lambda line: line.split(','))
        | beam.Map(lambda x: beam.Row(timestamp=x[0][:10],
                                      transaction_amount=float(x[3])))
        | beam.GroupBy('timestamp')
            .aggregate_field('transaction_amount',sum,'total_amount')
        | beam.Map(lambda x: f'{x.timestamp},{x.total_amount}')
        | beam.io.WriteToText(task3_out, 
                              file_name_suffix='.csv',
                              header='timestamp,total_amount')
        )
    
# task 4
with beam.Pipeline() as pipeline:
    agg = (
        pipeline
        | beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv', skip_header_lines=1)
        | beam.Map(lambda line: line.split(','))
        | beam.Map(lambda x: beam.Row(timestamp=x[0][:10],
                                      transaction_amount=float(x[3])))
        | beam.Filter(lambda x: float(x.transaction_amount) >= 20) 
        | beam.Filter(lambda x: x.timestamp >= '2010-01-01') 
        | beam.GroupBy('timestamp')
            .aggregate_field('transaction_amount',sum,'total_amount')
        | beam.Map(lambda x: f'{x.timestamp},{x.total_amount}')
        | beam.io.WriteToText(task4_out, 
                              file_name_suffix='.csv',
                              header='timestamp,total_amount')
        )