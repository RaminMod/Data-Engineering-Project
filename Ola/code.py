import h5py
import sys
import glob
import pyspark
import pyspark.pandas as ps
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
    Usage: 
        This application return the top 10 hottest songs from the the one million songs dataset.
    arguments:
        - the path to the datafolder
    """
    #Creating the Spark Session
    spark_session = SparkSession.builder\
    .master("spark://localhost:7077") \
    .appName("Top_Hottest_Songs_using_spark_standalone")\
    .config("spark.cores.max", 4)\
    .config("spark.driver.memory", "9g")\
    .getOrCreate()
    sc = spark_session.sparkContext
    #partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    data_folder_path = '10SongSubset/*.h5' #str(sys.argv[0]) +'/*/*/*/*.h5'
    #################################################
    # read a dataset and return it as a Python list #

    def f(path):
        with h5py.File(path) as f:
            data = f['/metadata/songs']
            column_names = list(data.dtype.names)
            return list(data[:])

    ################################################
    
    songs_hdf5_files_paths_rdd = sc.parallelize(list(glob.glob(data_folder_path)))

    songs_metadata_rdd = songs_hdf5_files_paths_rdd.flatMap(f)
    schema= ['analyzer_version', 'artist_7digitalid', 'artist_familiarity', 'artist_hotttnesss', 'artist_id', 
             'artist_latitude', 'artist_location', 'artist_longitude', 'artist_mbid', 'artist_name', 'artist_playmeid', 
             'genre', 'idx_artist_terms', 'idx_similar_artists', 'release', 'release_7digitalid', 
             'song_hotttnesss', 'song_id', 'title', 'track_7digitalid']
    #songs_metadata_df= spark_session.CreateDataFrame(songs_metadata_rdd, schema)

    songs_metadata_rdd.take(5)
    
