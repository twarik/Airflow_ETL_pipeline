# Created by twarik
import pandas as pd
import geopandas as gpd
import os, json, urllib

from sqlalchemy import create_engine
import psycopg2

from database_model import create_tables, create_spatial_table, create_tbl_from_DF

# Postgres database connection URI # [DB_FLAVOR]://[USERNAME]:[PASSWORD]@[DB_HOST]:[PORT]/[DB_NAME]
DATABASE_URI = 'postgres+psycopg2://postgres:pass1234@localhost:5432/hence_db'
engine = create_engine(DATABASE_URI)
# conn = engine.connect()
conn = psycopg2.connect("dbname=hence_db user=postgres password=pass1234 host=localhost")

# Path to staging area
temporary_storage = '/home/twarik/Desktop/airflow/files'

# [START extract_functions]
def extract1(**kwargs):
    """
    #### Extract function
    Reads data from CSV files stored in source.
    """
    #Connect to the original data source and retrieve data
    uga_dhs=pd.read_csv('https://raw.githubusercontent.com/twarik/data/main/uga_dhs_2016.csv')
    ug_clusters=pd.read_csv('https://raw.githubusercontent.com/twarik/data/main/ug_clusters.csv')
    health_sites=pd.read_csv('https://raw.githubusercontent.com/twarik/data/main/health_sites.csv')

    #save to temporary storage
    uga_dhs.to_csv(os.path.join(temporary_storage, "ug_dhs.csv"), index=False)
    ug_clusters.to_csv(os.path.join(temporary_storage, "ug_clusters.csv"), index=False)
    health_sites.to_csv(os.path.join(temporary_storage, "ug_dhs_sites.csv"), index=False)

def extract2(**kwargs):
    """
    #### Extract function
    Reads data from geoJSON file stored in source.
    """
    #Connect to the original data source and retrieve data
    uga_regions_gdf=gpd.read_file('https://raw.githubusercontent.com/twarik/data/main/uga_regions.geojson')


    #save to temporary storage
    uga_regions_gdf.to_file(os.path.join(temporary_storage, "ug_regions.geojson"), driver='GeoJSON')


def extract3(**kwargs):
    '''Extract function
    Function to query the DHS Program API for surveys data.
    Grabs data from the endpoint and saves to flat file on local storage
    '''
    # Get data from API
    #Connect to the original data source and retrieve data
    surveys_url = r'https://api.dhsprogram.com/rest/dhs/surveys'
    req = urllib.request.urlopen(surveys_url)
    resp = json.loads(req.read())

    #save to temporary storage
    with open(os.path.join(temporary_storage, "dhs_surveys.json"), "w") as outfile:
        json.dump(resp, outfile)

    # [END extract functions]

# [START transform_function]
def transform(**kwargs):
    """
    #### Transform function
    Reads data from local storage (data lake), processes it, and saves to new storage file
    """

    #Reads data from temporary storage
    dhs=pd.read_csv(os.path.join(temporary_storage, "ug_dhs.csv"))
    uga_regions_gdf=gpd.read_file(os.path.join(temporary_storage, "ug_regions.geojson"))
    with open(os.path.join(temporary_storage, "dhs_surveys.json"), 'r') as openfile:
        resp = json.load(openfile)

    #process the data and format it to make it suitable for analysis
    dhs['v535'] = dhs['v535'].astype('Int64')
    surveys_data = resp['Data']
    dhs_surveys_data = pd.DataFrame.from_dict(surveys_data)
    dhs_surveys_data.SurveyYear = pd.to_numeric(dhs_surveys_data.SurveyYear)

    #Save the processed data to staging area
    dhs.to_csv(os.path.join(temporary_storage, "ug_dhs_processed.csv"), index=False)
    uga_regions_gdf.to_file(os.path.join(temporary_storage, "ug_regions_processed.geojson"), driver='GeoJSON')
    dhs_surveys_data.to_csv(os.path.join(temporary_storage, "dhs_surveys_processed.csv"), index=False)
    # [END transform_function]

# [START load_function]
def load(**kwargs):
    """
    #### Load function
    Loading data into the data warehouse
    """
    create_tables()

    #Move standardized data to final storage (Data warehouse)
    copy_data_to_table_sql = '''
    COPY ug_clusters
    FROM '/home/twarik/Desktop/airflow/files/ug_clusters.csv'
    DELIMITER ','
    CSV HEADER;

    COPY ug_dhs_2016
    FROM '/home/twarik/Desktop/airflow/files/ug_dhs_processed.csv'
    DELIMITER ','
    CSV NULL '' HEADER;

    COPY health_sites (country, facility_name, facility_type, lat, long)
    FROM '/home/twarik/Desktop/airflow/files/ug_dhs_sites.csv'
    DELIMITER ','
    CSV HEADER;
    '''

    alter_table_structure_sql = '''
    ALTER TABLE ug_regions
    ALTER COLUMN name TYPE VARCHAR,
    ALTER COLUMN iso_code TYPE VARCHAR,
    ALTER COLUMN iso2_code TYPE CHAR(3),
    ALTER COLUMN area_type TYPE VARCHAR,
    ADD PRIMARY KEY (iso_code);

    ALTER TABLE ug_clusters ADD COLUMN geom geometry(Point, 4326);
    UPDATE ug_clusters SET geom = ST_SetSRID(ST_MakePoint(longnum, latnum), 4326);
    ALTER TABLE ug_clusters DROP COLUMN longnum, DROP COLUMN latnum;

    ALTER TABLE health_sites ADD COLUMN geom geometry(Point, 4326);
    UPDATE health_sites SET geom = ST_SetSRID(ST_MakePoint(long, lat), 4326);
    ALTER TABLE health_sites DROP COLUMN long, DROP COLUMN lat;
    '''

    dhs_surveys_df=pd.read_csv(os.path.join(temporary_storage, "dhs_surveys_processed.csv"))
    gdf=gpd.read_file(os.path.join(temporary_storage, "ug_regions_processed.geojson"))

    create_tbl_from_DF(dhs_surveys_df,table_name = 'dhs_surveys',engine=engine,behave='replace',index=False)
    create_spatial_table(GeoDataFrame=gdf,table_name="ug_regions",engine=engine, behave='replace')

    cur = conn.cursor()
    cur.execute(copy_data_to_table_sql)
    cur.execute(alter_table_structure_sql)
    conn.commit()
    cur.close()
    # conn.close()
    # [END load_function]
