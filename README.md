# Sentiment-Analysis-BERT-FedCommunication
This is a end to end machine learning solution that uses deep learning NLP models (BERT) to carry out aspect based sentiment analysis on Federal Reserve Communication. The communication medium can be written text (minutes, statements, surveys) or audio (live press conferences, speeches). Extensive use of Google Cloud Platform (GCP) to carry out data ingestion, data engineering, model training and model serving. Technologies used are: Apache Beam (DataFlow), Apache Airflow, TensorFlow, BERT, TPU's , GCP AI Platform


### AirFlow Dags
This folder contains AirFlow DAGs for Orchestration of Data Processing Tasks. The DailyProcessingDAG is run once a day and checks for any new events on the FOMC website through a web scapper. It triggers appropriate DAGS (minutesprocessing for example) as per the schedule of the new event. The MinutesProcessingDAG is one such DAG that processes Fed Minutes as soon as the document is released on the Federal Reserve website on the scheduled Date/Time. 

### ApacheBeam (Data Pipelines)
This folder contains  data processing pipelines developed using Apache Beam. Its flexible enough to be used for both Batch as well as Online Train/Prediction Jobs. During the model training phase, the pipeline was run on the GCP DataFlow runner to take advantage of Parallel Execution. In production/serving, it is run using the DirectRunner to save on processing time. Its created as a template in GCP DataFlow to allow for easier calling by AirFlow DAGs.

### Jupyter NoteBooks
This folder contains some experimental Jupyter notebooks.

### Trained BERT Model
A BERT model pretrained using a large corpus of Fed Communication (speeches, statements, minutes, beige books) etc is available on request. Given the size of the model checkpoint files (>1 GB), the free version of GitHub does not allow for it to be included in the repository
