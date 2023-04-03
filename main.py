
import os
import yaml
import json
from dateutil.parser import parse
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

project_id = ""
bucket_name = ""
dataset_id=""
table_id=""

def get_conf():
    with open('config.yml', 'r') as file:
        configs = yaml.safe_load(file)
    global project_id,bucket_name,file_name,dataset_id,table_id
    project_id=configs["project"]["project_id"]
    bucket_name=configs["project"]["bucket_name"]
    dataset_id=configs["project"]["dataset_id"] 
    table_id= project_id + "." + dataset_id + "." + configs["project"]["table_id"]     
   

def get_bucket_data(file_name):    
    tmp_name = 'tmp/' + file_name
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.get_bucket(bucket_name)     
    blob = storage.blob.Blob(file_name, bucket)
    text_blob = blob.download_as_text() 
    #pending call bia pub/sub
    #tmp_blob = bucket.rename_blob(blob, tmp_name)
    

    return text_blob

def move_blob_to_archive(file_name):    
    tmp_name = 'tmp/' + file_name
    archive_name = 'archive/' + file_name
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.get_bucket(bucket_name)     
    blob = storage.blob.Blob(tmp_name, bucket)    
    tmp_blob = bucket.rename_blob(blob, archive_name)   

    return tmp_blob.name

def get_bq_atomic_type(value):
    type_value = type(value)
    
    if type_value == list:
        return "RECORD"
    
    elif type_value == dict:
        return "RECORD"
    
    elif isinstance(type_value, bool):
        return "BOOLEAN"
    
    elif isinstance(value, int):
        return 'INTEGER'
   
    elif isinstance(value, float):
            return 'FLOAT'
            
    elif type_value == str:
        try:
            #check if is datetime or date 
            date = parse(value, fuzzy=True)
            if len(value)>10:
                return "TIMESTAMP"
            else:
                return "DATE"            
        except ValueError:            
            return "STRING"

    else:
            raise Exception(
                f'Unsupported type: {type(value)} '
            )

def blob_to_dict( json_text ):  
    
    json_file = json_text.split('\n')
    json_file = list(filter(lambda x: x.strip() != '', json_file))
    json_file = [json.loads(line) for line in json_file]    
            
    return json_file
   

def get_schema(generic_obj):
    schema=[]           
    
    for request in generic_obj:
        for k, v in request.items():
            field_base = None
            for field in schema:
                if field["name"] == k:
                    field_base = field
                    if get_bq_atomic_type(v) == field["type"]:
                        field["mode"] = "REQUIRED"                        
                    break
                           
            if not field_base:
                field_converted = {
                    "name": k,
                    "type": get_bq_atomic_type(v),
                    "mode": "NULLABLE"
                }
                
                if field_converted["type"] == "RECORD":
                    if isinstance(v, list):
                        field_converted["fields"] = get_schema(v)
                    else:
                        field_converted["fields"] = get_schema([v])
                schema.append(field_converted)
                
              
                    
    return schema  

def load_schema_into_table(final_schema,project_id):
    
    try:
        client = bigquery.Client(project=project_id)
        table = client.get_table(table_id)
        #Check if there is new fields
        #pending
        table.schema = final_schema
        table = client.update_table(table, ["schema"])
        return True
    except NotFound:
        #creates table in case not existing
        table = bigquery.Table(table_id,schema=final_schema)
        table = client.create_table(table) 
        return True           
    except:
          return False      

def load_data_into_table(project_id, dict_list):
    try:
        client = bigquery.Client(project=project_id)
        destination_table = client.get_table(table_id)
        number_rows_before = destination_table.num_rows
        load_job = client.load_table_from_json(dict_list,table_id)           
        load_job.result()
        destination_table = client.get_table(table_id)
        number_rows_loaded = destination_table.num_rows - number_rows_before        
        print("Loaded {} rows.".format(number_rows_loaded))
    except:
        print("error while loading")
            
def main(event, context):    
    get_conf()
    #get the name of the event sent to cloud function
    file_name = event["name"]          
    
    dict_list = blob_to_dict(get_bucket_data(file_name))
    final_schema = get_schema(dict_list)
    load_schema_into_table(project_id,final_schema)
    load_data_into_table(project_id,dict_list)
    
    #pending implementation of pubsub
    #final_blob = move_blob_to_archive(file_name)
    #print("Json file was moved to ", final_blob)
    
    
    

    
main({"name": "sample.json" },None )