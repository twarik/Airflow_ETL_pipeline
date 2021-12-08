import psycopg2

# connect to the PostgreSQL database
conn = psycopg2.connect("dbname=hence_db user=postgres password=pass1234 host=localhost")

def create_tables():
    """ create tables in the PostgreSQL database"""
    sql_commands = (
        """
        DROP SCHEMA public CASCADE;
        CREATE SCHEMA public;
        GRANT ALL ON SCHEMA public TO postgres;
        GRANT ALL ON SCHEMA public TO public;
        COMMENT ON SCHEMA public IS 'standard public schema';
        """,
        """
        --Creating ug_clusters table
        CREATE TABLE IF NOT EXISTS ug_clusters (
            v001 INT NOT NULL PRIMARY KEY,
            LATNUM double precision,
            LONGNUM double precision
        );
        """,
        """
        CREATE EXTENSION IF NOT EXISTS postgis;
        """,
        """
        --Creating ug_dhs_2016 table
        CREATE TABLE IF NOT EXISTS ug_dhs_2016 (
        	v001 INT NOT NULL,
        	v002 INT NOT NULL,
        	v003 INT NOT NULL,
        	v012 INT, v133 INT,
        	v535 BOOLEAN,
        	adult_radio_regular BOOLEAN,
        	v751 BOOLEAN,
        	sex VARCHAR,
        	PRIMARY KEY(v001, v002, v003),
            FOREIGN KEY (v001)
                REFERENCES ug_clusters(v001) ON UPDATE CASCADE
        );
        """,
        """
        --Creating health_sites table
        CREATE TABLE health_sites
        (
        	site_id SERIAL PRIMARY KEY,
        	country VARCHAR NOT NULL,
        	facility_name VARCHAR,
        	facility_type VARCHAR NOT NULL,
        	lat double precision,
        	long double precision
        );
        """
        )
    # conn = None
    try:
        cur = conn.cursor()
        # create table one by one
        for command in sql_commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        conn.close()
        # if conn is not None:
        #     conn.close()

def create_spatial_table(GeoDataFrame,table_name,engine,behave):
    GeoDataFrame.to_postgis(name=table_name, con=engine, if_exists=behave)

def create_tbl_from_DF(DataFrame,table_name,engine,behave,index):
    DataFrame.to_sql(name=table_name, con=engine, if_exists=behave, index=index)
