from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text, func

import pandas as pd

# Define the SQLHelper Class
# PURPOSE: Deal with all of the database logic

class SQLHelper():

    # Initialize PARAMETERS/VARIABLES

    #################################################
    # Database Setup
    #################################################
    def __init__(self):
        self.engine = create_engine("sqlite:///hawaii.sqlite")
        self.Station = self.createStation()
        self.Measurment = self.createMeasurement()

    # Used for ORM
    def createStation(self):
        # Reflect an existing database into a new model
        Base = automap_base()

        # reflect the tables
        Base.prepare(autoload_with=self.engine)

        # Save reference to the table
        Station = Base.classes.Station
        return(Station)
    
    def createMeasurement(self):
        # Reflect an existing database into a new model
        Base = automap_base()

        # reflect the tables
        Base.prepare(autoload_with=self.engine)

        # Save reference to the table
        Measurement = Base.classes.measurement
        return(Measurement)

##################################################################

    def queryPrecipitation(self):
        # Create our session (link) from Python to the DB
        session = Session(self.engine)
        conn = self.engine.connect()

        precip_query = text("""   
        SELECT date,prcp 
        FROM measurement
        WHERE date >= DATE('2017-08-23','-12 months')
        ORDER BY date ASC
                            """)

  
        df = pd.read_sql_query(precip_query,con=conn) 
        # Close the Session
        conn.close()
        return(df) 

#######################################################
 
def queryStations(self):

    session = Session(self.engine)
    conn = self.engine.connect()
 
 
    station_query = """
    SELECT COUNT(*) AS total_stations
    FROM station 
    """
    df2 = pd.read_sql_query(station_query, con=conn) 
    conn.close()
    return(df2) 
    

############################################################

def querytobs(self):
    session = Session(self.engine)
    conn = self.engine.connect() 

    total_query = """
    SELECT s.station,s.name, COUNT(m.station) AS station_count
    FROM measurement m
    JOIN station s ON m.station = s.station
    GROUP BY s.station
    ORDER BY station_count DESC
    """

     
    df3 = pd.read_sql_query(total_query, con=conn) 
    conn.close()
    return(df3) 

#############################################################################
def temp_query(self):
    session=Session(self.engine)
    conn = self.engine.connect() 

    temp_query = """
    SELECT s.station,s.name, COUNT(m.station) AS station_count, AVG(m.tobs) AS avg_temp, MIN(m.tobs) AS min_temp,MAX(m.tobs) as max_temp
    FROM measurement m
    JOIN station s ON m.station = s.station
    GROUP BY s.station
    ORDER BY station_count DESC
 """

    
    df4 = pd.read_sql_query(temp_query, con=conn) 

    conn.close()
    return(df4) 

##################################################################################
def query_precip(self):
    session=Session(self.engine)
    conn = self.engine.connect() 

    precip_query = """
    SELECT m.tobs, m.station
    FROM measurement m 
    WHERE m.station = "USC00519281"
    AND m.date >= DATE('2017-08-23', '-12 months')
    """

    df5 = pd.read_sql_query(precip_query, con=conn)
    

    conn.close()
    return(df5)
