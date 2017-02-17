import unicodedata
import os
import logging
from pyspark import SparkContext
from pyspark.mllib.recommendation import Rating, ALS


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
    @return Tuple of (year, title, clen title, id)
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

# Group cannonical movie names by year of release
log.debug("Grouping cannonical films by year")
grouped = canonical_data.groupByKey()
films_for_year = grouped.collectAsMap()

def find_netflix_alias(netflix_film):
    """
    Finds the cannonical name of this Netflix film
    @return Tuple of (netflix id, name), name is None if no alias was found
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
netflix_aliases = sc.broadcast(netflix_alias_rdd.collectAsMap())

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
#

log.info("Starting task 4")

# TODO
