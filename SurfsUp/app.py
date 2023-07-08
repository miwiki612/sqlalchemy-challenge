# Import the dependencies.

import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter

import numpy as np
import pandas as pd
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, text, inspect

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
measurement = Base.classes.measurement
station  = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)



#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Welcome to the Honolulu, Hawaii climate analysis API!<br/>"
        f"date format YYYY-MM-DD"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/{{start.date}}<br/>"
        f"/api/v1.0/{{start.date}}/{{end.date}}"
    )

@app.route("/api/v1.0/precipitation")
def year_precipitation():
    """Convert the query results from your precipitation analysis
    (i.e. retrieve only the last 12 months of data) to a dictionary using date as the key and prcp as the value."""
    
    #### precipitation analysis
    session = Session(engine)
    
    latest_date = session.query(measurement.date).order_by(measurement.date.desc()).first()
    latest_date = [ i for i in latest_date ][0]
    
    # Calculate the date one year from the last date in data set.
    start_date = dt.datetime.strptime( latest_date, "%Y-%m-%d") - dt.timedelta(days = 365)
    start_date = start_date.strftime("%Y-%m-%d")
    
    # Perform a query to retrieve the data and precipitation scores
    measure_prcp = session.query(measurement.date , measurement.prcp ).filter(measurement.date >= start_date ).all()
    
    # Close session
    session.close()
    
    # Save the query results as a Pandas DataFrame. Explicitly set the column names
    measure_prcp = pd.DataFrame(measure_prcp)
    measure_prcp.columns = ["date" ,"precipitation"]
    
    # Sort the dataframe by date
    measure_prcp = measure_prcp.sort_values( "date" , ascending= True )
    ####
    
    # json for return, date as key, precipitation as value
    year_return = {}   
    for i in range( len(measure_prcp) ):
        year_return[ measure_prcp.loc[i, "date"] ]=  measure_prcp.loc[i, "precipitation"]
    return jsonify(year_return)

@app.route("/api/v1.0/stations")
def stations():
    """Return a JSON list of stations from the dataset."""
    
    #### Require data from DB
    session = Session(engine)
    conn = engine.connect()
    # all station info
    stations_df = pd.read_sql("SELECT * FROM station", conn)
    # Close session
    session.close()
    ####
    
    # json for return, one dict for each row, column name as key
    col_names = stations_df.columns
    station_return = []
    for i in range(len(stations_df)):
        station_dict = {}
        for j in range(len(col_names)):
            station_dict[ col_names[j] ] = str(stations_df.iloc[ i , j]) # ERROR with int64, not JSON serializable
        station_return.append( station_dict )

    return jsonify( station_return )
    
@app.route("/api/v1.0/tobs")
def tobs():
    
    #### active station
    # Require data from DB
    session = Session(engine)
    
    # Design a query to retrieve the last 12 months of precipitation data and plot the results. 
    # Starting from the most recent data point in the database.
    latest_date = session.query(measurement.date).order_by(measurement.date.desc()).first()
    latest_date = [ i for i in latest_date ][0]
    
    # Calculate the date one year from the last date in data set.
    start_date = dt.datetime.strptime( latest_date, "%Y-%m-%d") - dt.timedelta(days = 365)
    start_date = start_date.strftime("%Y-%m-%d")
    
    # Most active station
    active_id = session.query( measurement.station ).group_by(measurement.station).order_by( func.count(measurement.station).desc() ).first()
    active_id = [ i for i in active_id ][0]
    
    # last 12 monthes for most active station
    active_temp = session.query( measurement.date, measurement.tobs).filter( measurement.station==active_id ).filter( measurement.date >= start_date ).all()
    
    # Close session
    session.close()
    ####
    
    # json for return, date as key, temperature as value
    active_temp = pd.DataFrame(active_temp)

    station_temp = {}
    for i in range( len(active_temp) ):
        station_temp[ active_temp.loc[i, "date"] ]=  active_temp.loc[i, "tobs"]
    
    return jsonify(station_temp)

@app.route("/api/v1.0/<start>", defaults={'end': dt.datetime.now().strftime("%Y-%m-%d") } )
@app.route("/api/v1.0/<start>/<end>")
def temp_range( start , end ):
    #### active station
    # Require data from DB
    session = Session(engine)
    
    # require temperature within the date range
    temp_data = session.query( func.min(measurement.tobs), func.avg(measurement.tobs) , func.max(measurement.tobs) ).\
    filter( measurement.date >= start ).filter( measurement.date <= end ).all()
    
    # Starting from the most recent data point in the database.
    latest_date = session.query(measurement.date).order_by(measurement.date.desc()).first()
    latest_date = [ i for i in latest_date ][0]
    
    # Starting from the eariliest data point in the database.
    earliest_date = session.query(measurement.date).order_by(measurement.date.asc()).first()
    earliest_date = [ i for i in earliest_date ][0]
    
    # Close session
    session.close()
    ####
    
    # adjust entered date to excist data date
    
    start_date = dt.datetime.strptime( start , "%Y-%m-%d")
    end_date = dt.datetime.strptime( end, "%Y-%m-%d")
    
    earliest_date = dt.datetime.strptime( earliest_date, "%Y-%m-%d")
    latest_date = dt.datetime.strptime( latest_date, "%Y-%m-%d")
    
    # actuall data range
    date_from = max( start_date , earliest_date)
    date_to = min( end_date , latest_date)
    
    # transfer TMIN, TAVG, and TMAX to list
    temp_data = temp_data[0]
    temp_data = [ i for i in temp_data ]
    
    if date_from <= date_to:
        temp_dict = {"minimum temperature" : round(temp_data[0],2) , \
                     "average temperature" : round(temp_data[1],2) , \
                     "maximum temperature" : round(temp_data[2],2) , 
                     "date_from" : date_from.strftime("%Y-%m-%d") , "date_to" : date_to.strftime("%Y-%m-%d") }
    # error return for empty date range
    else :
        temp_dict = { "error" : "no data"}
        
    return jsonify(temp_dict)

#################################################
if __name__ == '__main__':
    app.run(debug=True)