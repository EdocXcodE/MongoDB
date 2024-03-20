from connectServer import connect_to_mongodb

# Establish MongoDB connection
client = connect_to_mongodb()
db = client["mflix"]


# Function to retrieve top N movies with the highest IMDb rating
def top_movie_names_with_highest_imdb_rating(N):
    pipeline = [
        {
            '$match': {
                'imdb.rating': {'$exists': True, '$ne': ''}
            }
        },
        {
            '$sort': {'imdb.rating': -1}
        },
        {
            '$limit': N
        },
        {
            '$project': {'title': 1, 'imdb_rating': '$imdb.rating', '_id': 1}
        }
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)


# Function to retrieve top N movies with the highest IMDb rating in a specific year
def top_movie_names_with_highest_imdb_rating_in_year(N, year):
    pipeline = [
        {
            '$match': {'year': year}
        },
        {
            '$sort': {'imdb.rating': -1}
        },
        {
            '$limit': N
        },
        {
            '$project': {'title': 1, 'imdb_rating': '$imdb.rating', '_id': 1}
        }
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)


# Function to retrieve top N movies with the highest IMDb rating and votes above a threshold
def top_movie_names_with_highest_imdb_rating_votes(N, votes_threshold):
    pipeline = [
        {
            '$match': {'imdb.votes': {'$gt': votes_threshold}}
        },
        {
            '$sort': {'imdb.rating': -1}
        },
        {
            '$limit': N
        },
        {
            '$project': {'title': 1, 'imdb_rating': '$imdb.rating', '_id': 1, 'votes': '$imdb.votes'}
        }
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)


def top_movies_matching_pattern(N, pattern):
    pipeline = [
        {
            '$match': {
                'title': {'$regex': pattern, '$options': 'i'},  # Case-insensitive regex search for title pattern
                'tomatoes.viewer.rating': {'$exists': True}  # Filter movies with tomatoes rating
            }
        },
        {
            '$sort': {'tomatoes.viewer.rating': -1}  # Sort by highest tomatoes rating
        },
        {
            '$limit': N  # Limit the result to top N movies
        },
        {
            '$project': {'title': 1, 'tomatoes_rating': '$tomatoes.viewer.rating'}
            # Project only title and tomatoes rating
        }
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)


# Example usage of the functions
print("Top 5 movies with highest IMDB rating:\n")
data1 = top_movie_names_with_highest_imdb_rating(5)
for entry in data1:
    print(f"{entry['title']} - {entry['imdb_rating']}")

print("\nTop 3 movies with highest IMDB rating in 2001:\n")
data2 = top_movie_names_with_highest_imdb_rating_in_year(3, 2001)
for entry in data2:
    print(f"{entry['title']} - {entry['imdb_rating']}")

print("\nTop 10 movies with highest IMDB rating and votes > 1000:\n")
data3 = top_movie_names_with_highest_imdb_rating_votes(10, 1000)
for entry in data3:
    print(f"{entry['title']} - {entry['imdb_rating']} , votes : {entry['votes']}")

# Example usage
N = 10  # Number of movies to retrieve
pattern = "matrix"  # Pattern to search in movie titles

print(f"Top {N} movies with titles matching '{pattern}' sorted by highest tomatoes ratings:\n")
data = top_movies_matching_pattern(N, pattern)
for entry in data:
    print(f"{entry['title']} - Tomatoes Rating: {entry['tomatoes_rating']}")


# Function to retrieve top directors who created the maximum number of movies
def top_directors_max_movies(N):
    pipeline = [
        {
            '$unwind': '$directors'
        },
        {
            '$group': {
                '_id': '$directors',
                'total_movies': {'$sum': 1}
            }
        },
        {
            '$sort': {'total_movies': -1}
        },
        {
            '$limit': N
        }
    ]
    result = list(db.movies.aggregate(pipeline))
    return result


# Function to retrieve top directors who created the maximum number of movies in a specific year
def top_directors_max_movies_in_year(N, year):
    pipeline = [
        {
            '$match': {'year': year}
        },
        {
            '$unwind': '$directors'
        },
        {
            '$group': {
                '_id': '$directors',
                'total_movies': {'$sum': 1}
            }
        },
        {
            '$sort': {'total_movies': -1}
        },
        {
            '$limit': N
        }
    ]
    result = list(db.movies.aggregate(pipeline))
    return result


# Function to retrieve top directors who created the maximum number of movies for a specific genre
def top_directors_max_movies_for_genre(N, genre):
    pipeline = [
        {
            '$match': {'genres': genre}
        },
        {
            '$unwind': '$directors'
        },
        {
            '$group': {
                '_id': '$directors',
                'total_movies': {'$sum': 1}
            }
        },
        {
            '$sort': {'total_movies': -1}
        },
        {
            '$limit': N
        }
    ]
    result = list(db.movies.aggregate(pipeline))
    return result


# Example usage of the director-related functions
print("\nTop directors who created the maximum number of movies:\n")
data1 = top_directors_max_movies(5)
for entry in data1:
    print(f"{entry['_id']} - {entry['total_movies']}")

print("\nTop directors who created the maximum number of movies in 2001:\n")
data2 = top_directors_max_movies_in_year(5, 2001)
for entry in data2:
    print(f"{entry['_id']} - {entry['total_movies']}")

print("\nTop directors who created the maximum number of movies for the genre 'Action':\n")
data3 = top_directors_max_movies_for_genre(5, 'Action')
for entry in data3:
    print(f"{entry['_id']} - {entry['total_movies']}")


# Function to retrieve top actors who starred in the maximum number of movies
def top_actors_max_movies(N):
    pipeline = [
        {
            '$unwind': '$cast'
        },
        {
            '$group': {
                '_id': '$cast',
                'count': {'$sum': 1}
            }
        },
        {
            '$sort': {'count': -1}
        },
        {
            '$limit': N
        }
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)


# Function to retrieve top actors who starred in the maximum number of movies in a given year
def top_actors_max_movies_year(N, year):
    pipeline = [
        {
            '$match': {'year': year}
        },
        {
            '$unwind': '$cast'
        },
        {
            '$group': {
                '_id': '$cast',
                'count': {'$sum': 1}
            }
        },
        {
            '$sort': {'count': -1}
        },
        {
            '$limit': N
        }
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)


# Function to retrieve top actors who starred in the maximum number of movies for a given genre
def top_actors_max_movies_genre(N, genre):
    pipeline = [
        {
            '$match': {'genres': genre}
        },
        {
            '$unwind': '$cast'
        },
        {
            '$group': {
                '_id': '$cast',
                'count': {'$sum': 1}
            }
        },
        {
            '$sort': {'count': -1}
        },
        {
            '$limit': N
        }
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)


# Function to retrieve top N movies for each genre with the highest IMDb rating
def top_movies_per_genre(N):
    pipeline = [
        {'$match': {
            'imdb.rating': {'$exists': True, '$ne': ''}
        }
        },
        {
            '$unwind': '$genres'
        },
        {
            '$sort': {'imdb.rating': -1}
        },
        {
            '$group': {
                '_id': '$genres',
                'movies': {'$push': {'title': '$title', 'id': '$_id', 'imdb_rating': '$imdb.rating'}}
            }
        },
        {
            '$project': {
                'genre': '$_id',
                'top_movies': {'$slice': ['$movies', N]},
                '_id': 0
            }
        }
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)


# Example usage of the actor-related and genre-related functions
N = 3
year = 2002
genre = 'Action'

print("\nTop", N, "actors who starred in the maximum number of movies:\n")
data1 = top_actors_max_movies(N)
for entry in data1:
    print(f"{entry['_id']} - {entry['count']}")

print("\nTop", N, "actors who starred in the maximum number of movies in", year, ":\n")
data2 = top_actors_max_movies_year(N, year)
for entry in data1:
    print(f"{entry['_id']} - {entry['count']}")

print("\nTop", N, "actors who starred in the maximum number of movies for the genre", genre, ":\n")
data3 = top_actors_max_movies_genre(N, genre)
for entry in data3:
    print(f"{entry['_id']} - {entry['count']}")

print("\nTop", N, "movies for each genre with the highest IMDB rating:\n")
data4 = top_movies_per_genre(N)

for entry in data4:
    print(f"\n{entry['genre']} : \n")
    for gen in entry['top_movies']:
        print(f"{gen['title']} - {gen['imdb_rating']} - {gen['id']}")


def top_movies_matching_pattern(N, pattern):
    pipeline = [
        {
            '$match': {
                'title': {'$regex': pattern, '$options': 'i'},  # Case-insensitive regex search for title pattern
                'tomatoes.viewer.rating': {'$exists': True}  # Filter movies with tomatoes rating
            }
        },
        {
            '$sort': {'tomatoes.viewer.rating': -1}  # Sort by highest tomatoes rating
        },
        {
            '$limit': N  # Limit the result to top N movies
        },
        {
            '$project': {'title': 1, 'tomatoes_rating': '$tomatoes.viewer.rating'}
            # Project only title and tomatoes rating
        }
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)


# Example usage
N = 10  # Number of movies to retrieve
pattern = "matrix"  # Pattern to search in movie titles

print(f"Top {N} movies with titles matching '{pattern}' sorted by highest tomatoes ratings:\n")
data = top_movies_matching_pattern(N, pattern)
for entry in data:
    print(f"{entry['title']} - Tomatoes Rating: {entry['tomatoes_rating']}")
