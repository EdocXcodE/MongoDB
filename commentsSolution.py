from connectServer import connect_to_mongodb
from datetime import datetime

# Connect to MongoDB
client = connect_to_mongodb()
db = client["mflix"]


# Find top 10 users who made the maximum number of comments
def top_10_users_with_most_comments():
    # Aggregation pipeline to group comments by user email and count the total comments
    # Then sort by total comments in descending order and limit to 10 results
    pipeline = [
        {"$group": {"_id": "$email", "total_comments": {"$sum": 1}}},
        {"$sort": {"total_comments": -1}},
        {"$limit": 10}
    ]
    result = db.comments.aggregate(pipeline)
    return list(result)


# Find top 10 movies with most comments
def top_10_movies_with_most_comments():
    # Aggregation pipeline to group comments by movie and count the total comments
    # Also, performs a lookup to get movie titles from the movies collection
    pipeline = [
        {"$group": {"_id": "$movie_id", "total_comments": {"$sum": 1}}},
        {"$lookup": {"from": "movies", "localField": "_id", "foreignField": "_id", "as": "movie"}},
        {"$unwind": "$movie"},
        {"$project": {"_id": "$_id", "title": "$movie.title", "total_comments": "$total_comments"}},
        {"$sort": {"total_comments": -1}},
        {"$limit": 10}
    ]

    result = db.comments.aggregate(pipeline)
    res_final = []
    for movie in result:
        res_final.append({"title: ": movie['title'],
                          "total_comments: ": movie['total_comments']})
    return res_final


# Given a year, find the total number of comments created each month in that year
def total_comments_by_month(year):
    # Aggregation pipeline to match comments created within a given year
    # Group comments by month and count the total comments for each month
    pipeline = [
        {
            '$match': {
                'date': {
                    '$gte': datetime(year, 1, 1),
                    '$lt': datetime(year + 1, 1, 1)
                }
            }
        },
        {
            '$group': {
                '_id': {'$month': '$date'},
                'total_comments': {'$sum': 1}
            }
        },
        {
            '$sort': {'_id': 1}
        }
    ]

    result = db.comments.aggregate(pipeline)
    return list(result)


# Example usage:
print("Top 10 users with most comments:\n")
data1 = top_10_users_with_most_comments()
for entry in data1:
    print(f"{entry['_id']} - {entry['total_comments']}")

print("\nTop 10 movies with most comments:\n")
data2 = top_10_movies_with_most_comments()
for entry in data2:
    print(f"{entry['title: ']} - {entry['total_comments: ']}")

print("\nTotal comments per month in 2000:\n")
data3 = total_comments_by_month(2000)
for entry in data3:
    print(f"{entry['_id']} - {entry['total_comments']}")
