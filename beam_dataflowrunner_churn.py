import argparse
import json
import csv
import logging
import numpy as np
import pandas as pd
import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

from sklearn.preprocessing import LabelEncoder, OneHotEncoder
from sklearn.preprocessing import MinMaxScaler

from sklearn.model_selection import train_test_split
from sklearn.linear_model import RidgeClassifier
from sklearn.metrics import accuracy_score
from sklearn.metrics import confusion_matrix

import pickle

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pickle,logging,csv
import argparse
import json
import csv
import io
import numpy as np
import pandas as pd
import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import cloudstorage as gcs

PROJ_NAME = 'tiger-mle'
RUNNER = 'DataflowRunner'
JOB_NAME = 'test1'
TEMP_LOCATION = 'gs://beampipeline/temp'
OUTPUT_PATH = 'gs://beampipeline/output'

def preprocess(df):
    import pandas as pd
    import numpy as np
    # df=pd.read_csv('Telco-Customer-Churn.csv')
    # df=pd.read_csv("gs://beampipeline/data/Telco-Customer-Churn.csv")
    columns = df.columns
    binary_cols = []
    for col in columns:
        if df[col].value_counts().shape[0] == 2:
            binary_cols.append(col)
    # Categorical features with multiple classes
    multiple_cols_cat = ['MultipleLines', 'InternetService', 'OnlineSecurity', 'OnlineBackup',
     'DeviceProtection', 'TechSupport', 'StreamingTV', 'StreamingMovies', 'Contract','PaymentMethod']
    churn_numeric = {'Yes':1, 'No':0}
    df.Churn.replace(churn_numeric, inplace=True)
    df.drop(['customerID','gender','PhoneService','Contract','TotalCharges'], axis=1, inplace=True)
    cat_features = ['SeniorCitizen', 'Partner', 'Dependents',
        'MultipleLines', 'InternetService', 'OnlineSecurity',
       'OnlineBackup', 'DeviceProtection', 'TechSupport', 'StreamingTV',
       'StreamingMovies', 'PaperlessBilling', 'PaymentMethod']
    X = pd.get_dummies(df, columns=cat_features, drop_first=True)
    sc = MinMaxScaler()
    a = sc.fit_transform(df[['tenure']])
    b = sc.fit_transform(df[['MonthlyCharges']])
    X['tenure'] = a
    X['MonthlyCharges'] = b
    X_no = X[X.Churn == 0]
    X_yes = X[X.Churn == 1]
    X_yes_upsampled = X_yes.sample(n=len(X_no), replace=True, random_state=42)
    X_upsampled = X_no.append(X_yes_upsampled).reset_index(drop=True)
    X = X_upsampled.drop(['Churn'], axis=1) #features (independent variables)
    y = X_upsampled['Churn'] #target (dependent variable)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state=42)
    x=X_test.to_numpy()
    return [x]

def pred(d):
    import pandas as pd
    import numpy as np
    import apache_beam as beam
    import pickle
    # d=pd.DataFrame(d)
    # print(d)
    model=None
    with beam.io.gcsio.GcsIO().open("gs://beampipeline/ChurnModel/saved_model.pkl",'rb') as f:
        model=pickle.load(f)#open("saved_model.pkl",'rb')
        f.close()
    # model=pickle.load(open("gs://beampipeline/Models/saved_model.pkl", 'rb'))
    out=[]
    for i in d:
        ans=model.predict([i])
        logging.info("predictions:{}".format(ans))
        out.append(ans)
    newdata=list()
    for line in out:
        newdata.append(line[0])
    # print(newdata)
    return newdata
def change(d):
    d1={}
    d1['prediction']=str(d[0:10])
    print(d1)
    return d1

class My_class(beam.DoFn):
    def __init__(self,df):
        self.df=df
    def process(self,message):
        import pandas as pd
        import numpy as np
        from sklearn.preprocessing import LabelEncoder, OneHotEncoder
        from sklearn.preprocessing import MinMaxScaler
        from sklearn.model_selection import train_test_split

        # df=pd.read_csv("gs://beampipeline/data/Telco-Customer-Churn.csv")
        columns = self.df.columns
        binary_cols = []
        for col in columns:
            if self.df[col].value_counts().shape[0] == 2:
                binary_cols.append(col)
        # Categorical features with multiple classes
        multiple_cols_cat = ['MultipleLines', 'InternetService', 'OnlineSecurity', 'OnlineBackup',
         'DeviceProtection', 'TechSupport', 'StreamingTV', 'StreamingMovies', 'Contract','PaymentMethod']
        churn_numeric = {'Yes':1, 'No':0}
        self.df.Churn.replace(churn_numeric, inplace=True)
        self.df.drop(['customerID','gender','PhoneService','Contract','TotalCharges'], axis=1, inplace=True)
        cat_features = ['SeniorCitizen', 'Partner', 'Dependents',
            'MultipleLines', 'InternetService', 'OnlineSecurity',
           'OnlineBackup', 'DeviceProtection', 'TechSupport', 'StreamingTV',
           'StreamingMovies', 'PaperlessBilling', 'PaymentMethod']
        X = pd.get_dummies(self.df, columns=cat_features, drop_first=True)
        sc = MinMaxScaler()
        a = sc.fit_transform(self.df[['tenure']])
        b = sc.fit_transform(self.df[['MonthlyCharges']])
        X['tenure'] = a
        X['MonthlyCharges'] = b
        X_no = X[X.Churn == 0]
        X_yes = X[X.Churn == 1]
        X_yes_upsampled = X_yes.sample(n=len(X_no), replace=True, random_state=42)
        X_upsampled = X_no.append(X_yes_upsampled).reset_index(drop=True)
        X = X_upsampled.drop(['Churn'], axis=1) #features (independent variables)
        y = X_upsampled['Churn'] #target (dependent variable)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state=42)
        x=X_test.to_numpy()
        yield x
        
import csv
import pandas as pd
import apache_beam as beam
pipeline_args = [
      '--runner', RUNNER,
      '--project', PROJ_NAME,
      '--temp_location', TEMP_LOCATION,
      '--job_name', JOB_NAME,
      '--region','us-east1',
      '--service_account_email', 'vertexai-explore@tiger-mle.iam.gserviceaccount.com',
      '--staging_location','gs://beampipeline/staging',
      '--experiment','use_runner_v2',
      '--experiment','use_unsupported_python_version'
]
   
df=pd.read_csv('gs://beampipeline/data/Telco-Customer-Churn.csv')

options = PipelineOptions(pipeline_args,save_main_session=False, streaming=False)

with beam.Pipeline(options=options) as pipeline:
    # pcoll = pipeline | 'Create data' >> beam.Create(event)
    p=(
          pipeline

          # Create a single element containing the entire PCollection. 
          | 'Singleton' >> beam.Create([None])
          | 'Preprocess' >> beam.ParDo(My_class(df))
          | 'prediction'>>beam.Map(pred)
          | beam.Map(change)
        # |beam.ParDo(print)
          |"writetobq">> beam.io.WriteToBigQuery(
                table="tiger-mle:pipeline.test",
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
    )
