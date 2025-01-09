import pandas as pd
import flask as fk
from flask import Flask, jsonify
from sql_helper import SQLHelper


app = Flask(__name__)

sqlHelper = SQLHelper()

  
#################################################
# Flask Setup
#################################################
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"<a href='/api/v1.0/temperature' target='_blank'>/api/v1.0/temperature</a><br/>"
        f"<a href='/api/v1.0/2017-01-01' target='_blank'>/api/v1.0/2017-01-01</a><br/>"
        f"<a href='/api/v1.0/2017-01-01/2017-01-31' target='_blank'>/api/v1.0/2017-01-01/2017-01-31</a><br/>"
    )



@app.route("/api/v1.0/precipitation")
def precipitation():
    # Execute Query
    df = sqlHelper.queryPrecipitation()

    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data)

@app.route("/api/v1.0/stations")
def stations():
    # Execute Query
    df = sqlHelper.queryStations()

    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data)



@app.route("/api/v1.0/tobs")
def temperature():
    # Execute Query
    df = sqlHelper.querytobs()

    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data)



@app.route("/api/v1.0/<start>")
def temp_query(start):
    # Execute Query
    df = sqlHelper.queryTStatsSQL(start)

    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data)



@app.route("/api/v1.0/<start>/<end>")
def precip_query(start, end):
    # Execute Query
    df = sqlHelper.precip_query(start, end)

    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data)


if __name__ == '__main__':
    app.run(debug=True)
