# Importing Necessary dependencies
import pandas as pd
import json

def run_transformation():
    real_estate_df = pd.read_json('real_estate.json')

    ### Transformation Layer
    location_dim = real_estate_df[['id', 'longitude', 'latitude', 'addressLine1', 'city', 'state', 'zipCode', 'formattedAddress',
                                    'addressLine2', 'county',]].copy().drop_duplicates().reset_index(drop=True)
                                    
    location_dim.index.name = 'location_id'
    location_dim = location_dim.reset_index()

    features_dim = pd.json_normalize(real_estate_df['features'])
    features_dim['id'] = real_estate_df['id']
    features_dim['bathrooms'] = real_estate_df['bathrooms']
    features_dim['bedrooms'] = real_estate_df['bedrooms']
    features_dim['squareFootage'] = real_estate_df['squareFootage']
    features_dim.index.name = 'features_id'
    features_dim = features_dim.reset_index()

    sales_dim = real_estate_df[['id', 'lastSalePrice', 'lastSaleDate']].copy().drop_duplicates().reset_index(drop=True)
    sales_dim.index.name = 'sales_id'
    sales_dim = sales_dim.reset_index()

    owner = pd.json_normalize(real_estate_df['owner'])
    owner['id'] = real_estate_df['id']
    owner = owner[['id', 'names', 'mailingAddress.id', 'mailingAddress.addressLine1',
        'mailingAddress.city', 'mailingAddress.state', 'mailingAddress.zipCode',
        'mailingAddress.addressLine2', 'mailingAddress.formattedAddress']]
    owner.index.name = 'owner_id'
    owner = owner.reset_index()

    propertyTaxes = pd.json_normalize(real_estate_df['propertyTaxes'])
    propertyTaxes['id'] = real_estate_df['id']
    propertyTaxes = propertyTaxes[['id','2021.total', '2022.total', '2020.total', '2023.total', '2018.total',
        '2019.total', '2017.total', '2016.total' ]]
    propertyTaxes.index.name = 'propertyTaxes_id'
    propertyTaxes = propertyTaxes.reset_index()


    tax_assessment = pd.json_normalize(real_estate_df['taxAssessment'])
    tax_assessment['id'] = real_estate_df['id']
    tax_assessment = tax_assessment[['id', '2022.value', '2022.land', '2022.improvements', '2021.value',
        '2021.land', '2021.improvements', '2020.value', '2020.land',
        '2020.improvements', '2023.value', '2023.land', '2023.improvements',
        '2019.value', '2019.land', '2019.improvements', '2018.value',
        '2018.land', '2018.improvements', '2017.value', '2017.land',
        '2017.improvements', '2024.value']]

    tax_assessment.index.name = 'tax_assessment_id'
    tax_assessment = tax_assessment.reset_index()

    property_fact = real_estate_df.merge(location_dim, on=['id', 'longitude', 'latitude', 'addressLine1', 'city', \
                                    'state', 'zipCode', 'formattedAddress', 'addressLine2', 'county'], how='left') \
                                .merge(features_dim, on=['id', 'bathrooms', 'bedrooms', 'squareFootage'], how='left') \
                                .merge(sales_dim, on=['id', 'lastSalePrice', 'lastSaleDate'], how='left') \
                                .merge(owner, on='id', how='left') \
                                .merge(propertyTaxes, on='id', how='left') \
                                .merge(tax_assessment, on='id', how='left') \
                                [['id','location_id', 'features_id', 'sales_id', 'owner_id', 'propertyTaxes_id', 'tax_assessment_id', 'yearBuilt', \
                                    'assessorID', 'legalDescription', 'subdivision', 'zoning', 'lotSize']]

    
    #save to local memory
    location_dim.to_csv(r'dataset/location_dim.csv', index=False)
    sales_dim.to_csv(r'dataset/sales_dim.csv', index=False)
    features_dim.to_csv(r'dataset/features_dim.csv', index=False)
    owner.to_csv(r'dataset/owner.csv', index=False)
    propertyTaxes.to_csv(r'dataset/propertyTaxes.csv', index=False)
    tax_assessment.to_csv(r'dataset/tax_assessment.csv', index=False)
    property_fact.to_csv(r'dataset/property_fact.csv', index=False)