import logging
import os
import unicodedata
from neo4j.v1 import GraphDatabase
from pyspark import SparkContext
from pyspark.mllib.recommendation import Rating, ALS
from pyspark.mllib.evaluation import RegressionMetrics, RankingMetrics


# Configure logging
logging.basicConfig(format="%(asctime)s: [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


def run_from_ipython():
    """
    Test if the script was run from IPython.
    """
    try:
        __IPYTHON__
        return True
    except NameError:
        return False


# Assume that if we are running from an IPython context it is one that already
# passed in a SparkContext.
# This allows the script to be run from IPython using : %run -r cw2.py
if not run_from_ipython():
    sc = SparkContext("local", "CW2")
    sc.setLogLevel(WARN)


root_directory = '/home/ubuntu/BigDataAnalysis/data'

# Datasets
netflix_raw_data = sc.textFile(os.path.join(root_directory, 'netflix_movie_titles.txt'))
canonical_raw_data = sc.textFile(os.path.join(root_directory, 'movie_titles_canonical.txt'))
netflix_raw_ratings = sc.textFile(os.path.join(root_directory, 'mv_all_simple.txt'))
qualification_raw_data = sc.textFile(os.path.join(root_directory, 'qualifying_simple.txt'))


#
# TASK 1
# Loading Netflix and cannonical movie names and building alias map
#

log.info("Starting task 1")

def clean_title_string(title):
    """
    Remove accents, symbols and whitespace and convert to lowercase
    (http://stackoverflow.com/questions/517923/what-is-the-best-way-to-remove-accents-in-a-python-unicode-string)
    """
    nfkd_form = unicodedata.normalize('NFKD', title)
    return u"".join([c.lower() for c in nfkd_form if c.isalnum() and not unicodedata.combining(c)])

def parse_netflix_data(row):
    """
    Parse a row (string) form the netflix title dataset.
    @return Tuple of (year, title, clen title, ID)
    """
    data = row.split(',')

    title = data[2].strip()
    try:
        year = int(data[1])
    except:
        year = 0

    return (year, title, clean_title_string(title), int(data[0]))

def parse_canonical_data(row):
    """
    Parse a row (string) form the canonical movie title dataset.
    @return Tuple of (year, (title, clen title))
    """
    data = row.rpartition(',')

    title = data[0].strip()
    try:
        year = int(data[2])
    except:
        year = 0

    return (year, (title, clean_title_string(title)))

# Parse raw data
log.debug("Parsing raw name datasets")
netflix_data = netflix_raw_data.map(parse_netflix_data)
canonical_data = canonical_raw_data.map(parse_canonical_data)

# Group canonical movie names by year of release
log.debug("Grouping canonical films by year")
grouped = canonical_data.groupByKey()
films_for_year = grouped.collectAsMap()

def find_netflix_alias(netflix_film):
    """
    Finds the canonical name of this Netflix film
    @return Tuple of (Netflix ID, name), name is None if no alias was found
    """
    title = netflix_film[1]
    year = netflix_film[0]

    if year in films_for_year:
        title_results = [f for f in films_for_year[netflix_film[0]] if f[1] == netflix_film[2]]
        if len(title_results) == 1:
            title = title_results[0][0]

    return (netflix_film[3], title)


# Get map of Netflix title aliases (filter out those with no alias)
log.debug("Gets map of Netflix IDs to cannonical names")
netflix_alias_rdd = netflix_data.map(find_netflix_alias).filter(lambda f: f[1] is not None)

log.info("Completed task 1")


#
# TASK 2
# Train model for generating recommendations
#

log.info("Starting task 2")

def parse_netflix_ratings(row):
    """
    Parses a row in the Netflix rating dataset
    Rating dates are discarded
    @return Rating
    """
    data = row.split(',')
    return Rating(int(data[1]), int(data[0]), int(data[2]))

# Parse raw Netflix rating dataset
log.debug("Parsing Netflix ratings dataset")
netflix_ratings = netflix_raw_ratings.map(parse_netflix_ratings)

# Persist this as it will be used frequently
netflix_ratings.persist()

# Randomly split Netflix ratings into training and testing sets
log.debug("Splitting Netflix ratings RDD")
training_set, testing_set = netflix_ratings.randomSplit(weights=[0.8, 0.2])

# Train model
log.debug("Starting model training")
model = ALS.train(training_set, rank=10, iterations=5, lambda_=0.01)

log.info("Completed task 2")


#
# TASK 3
# Generating recommendations for a given user
#

log.info("Starting task 3")

guinea_pig_user_id = 30878

# Recommend Netflix items for user
log.debug("Generating recommendations for user")
recommendations = model.recommendProducts(guinea_pig_user_id, 10)

# Broadcast to workers (this seems to fail if done before model training)
netflix_aliases = sc.broadcast(netflix_alias_rdd.collectAsMap())

def netflix_id_to_film_name(r):
    return (netflix_aliases.value[r.product], r.rating)

# Get recommendations from model
recommended_films = map(netflix_id_to_film_name, recommendations)

# Save recommendations to file
log.debug("Saving user's recommendations")
with open(os.path.join(root_directory, 'recommendations_for_{}.txt'.format(guinea_pig_user_id)), 'w') as f:
    for r in recommended_films:
        f.write('{} ({})\n'.format(r[0], r[1]))

# Get some genuine ratings by the user
log.debug("Sampling actual ratings for user")
user_ratings = netflix_ratings.filter(lambda r: r.user == guinea_pig_user_id).sample(False, 0.1).map(netflix_id_to_film_name).take(10)

# Save sample of users ratings to file
log.debug("Saving sample of user's actual ratings")
with open(os.path.join(root_directory, 'ratings_from_{}.txt'.format(guinea_pig_user_id)), 'w') as f:
    for r in user_ratings:
        f.write('{} ({})\n'.format(r[0], r[1]))

log.info("Completed task 3")


#
# TASK 4
# Computationally evaluating model fitness
#

log.info("Starting task 4")

# Create model evaluation dataset
evaluation_data = testing_set.map(lambda p: (p.user, p.product))

# Predictions for evaluation
# Returns an RDD of tuple ((user ID, film ID), rating)
log.debug("Creating predictions for evaluation")
predictions = model.predictAll(evaluation_data).map(lambda r: ((r.user, r.product), r.rating))

# True ratings to compare to
# Returns an RDD of tuple ((user ID, film ID), rating)
log.debug("Creating true comparison dataset")
ratings_tuple = testing_set.map(lambda r: ((r.user, r.product), r.rating))

# Join predictions and true data for evaluation
log.debug("Joining prediction and true data")
comparison_join = predictions.join(ratings_tuple)

# Obtain pairs of predictions and true ratings
log.debug("Creating comparison pairs")
comparison_pairs = comparison_join.map(lambda r: r[1])

# Create metrics
log.debug("Creating fitness metrics")
metrics = RegressionMetrics(comparison_pairs)

# Calculate root mean square error
log.debug("Calculating root mean square error")
rmse = metrics.rootMeanSquaredError

print("Root Mean Square error = {}".format(rmse))

log.info("Completed task 4")


#
# TASK 5
# Generate predictions for a qualification dataset
#

log.info("Starting task 5")

def parse_qualification_data(row):
    """
    Parse a row (string) form the qualification dataset.
    @return Tuple of (user ID, Netflix ID)
    """
    data = row.split(',')
    return (int(data[0]), int(data[1]))

# Parse qualification data
log.debug("Parsing qualification dataset")
qualification_data = qualification_raw_data.map(parse_qualification_data)

# Generate predictions for all qualification data
log.debug("Generating predictions for qualification dataset")
qualification_ratings = model.predictAll(qualification_data)

# Save predictions
log.debug("Saving qualification predictions")
qualification_ratings.saveAsTextFile(os.path.join(root_directory, 'qualification_predictions'))

log.info("Completed task 5")


#
# TASK 6
# Insert ratings into Neo4j graph DB
#

log.info("Starting task 6")

# Generate tuples with canonical names ready for insertion into database from smaple of ratings
log.debug("Generating data for DB from ratings sample set")
qualification_ratings_for_graph_db = qualification_ratings.sample(False, 0.05).map(lambda r: (r.user, netflix_aliases.value[r.product], r.rating))

# TODO

log.info("Completed task 6")

