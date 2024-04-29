# Importing Necessary dependencies
import pandas as pd
import json
import io
import os
from azure.storage.blob import BlobClient, BlobServiceClient
import os
secret_key = os.environ.get("AZURE_STORAGE_KEY")


def run_loading():
    #save to local memory
    real_estate_df = pd.read_json(r'real_estate.json')
    location_dim = pd.read_csv(r'dataset/location_dim.csv')
    sales_dim = pd.read_csv(r'dataset/sales_dim.csv')
    features_dim = pd.read_csv(r'dataset/features_dim.csv')
    owner = pd.read_csv(r'dataset/owner.csv')
    propertyTaxes = pd.read_csv(r'dataset/propertyTaxes.csv')
    tax_assessment = pd.read_csv(r'dataset/tax_assessment.csv')
    property_fact = pd.read_csv(r'dataset/property_fact.csv')

    ### Loading Layer
    connect_str = secret_key
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_name = 'realyocontainer'
    container_client = blob_service_client.get_container_client(container_name)

    def upload_df_to_blob_parquet(df, container_client, blob_name):
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(buffer, blob_type="BlockBlob", overwrite=True)
        print(f'{blob_name} uploaded to Blob Storage successfully')

    upload_df_to_blob_parquet(real_estate_df, container_client, 'rawdata/real_estate.parquet')

    upload_df_to_blob_parquet(location_dim, container_client, 'cleaneddata/location_dim.parquet')
    upload_df_to_blob_parquet(sales_dim, container_client, 'cleaneddata/sales_dim.parquet')
    upload_df_to_blob_parquet(features_dim, container_client, 'cleaneddata/features_dim.parquet')
    upload_df_to_blob_parquet(owner, container_client, 'cleaneddata/owner.parquet')
    upload_df_to_blob_parquet(propertyTaxes, container_client, 'cleaneddata/propertyTaxes.parquet')
    upload_df_to_blob_parquet(tax_assessment, container_client, 'cleaneddata/tax_assessment.parquet')
    upload_df_to_blob_parquet(property_fact, container_client, 'cleaneddata/property_fact.parquet')