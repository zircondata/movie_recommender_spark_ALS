import time
import sys
import cherrypy
import os
from pyspark import SparkContext, SparkConf
from app import create_app

def init_spark_context():
    conf = SparkConf().setAppName("movie_recommendation-server")  # load spark context
    sc = SparkContext(conf=conf, pyFiles=['engine.py', 'app.py']) # IMPORTANT: pass aditional Python modules to each worker
    return sc
def run_server(app):
    cherrypy.tree.graft(app.wsgi_app, '/')   # Set the configuration of the web server
    cherrypy.config.update({'engine.autoreload.on': True,
                            'log.screen': True,
                            'server.socket_port': 5432,
                            'server.socket_host': '0.0.0.0'})
    cherrypy.engine.start()  # Start the CherryPy WSGI web server
    #cherrypy.engine.block()

if __name__ == "__main__":
    sc = init_spark_context()           # Init spark context and load libraries
    movies_set_path = sys.argv[1] if len(sys.argv) > 1 else r"C:\Users\DELL\SPARK-MOVIE\app\ml-latest\movies.csv"
    ratings_set_path = sys.argv[2] if len(sys.argv) > 2 else r"C:\Users\DELL\SPARK-MOVIE\app\ml-latest\ratings.csv"
    app = create_app(sc, movies_set_path, ratings_set_path)
    run_server(app) # start web server