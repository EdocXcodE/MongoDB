from connectServer import connect_to_mongodb
from bson.son import SON  # Import SON for sorting
from math import radians, sin, cos, sqrt, atan2

# Connect to MongoDB
client = connect_to_mongodb()
db = client["mflix"]


def top_cities_with_theatres(limit):
    """
    Find the top cities with the maximum number of theatres.

    Args:
        limit (int): The maximum number of cities to return.

    Returns:
        list: A list of dictionaries containing city names and the number of theatres.

    """
    pipeline = [
        {"$group": {"_id": "$location.address.city", "theatres_count": {"$sum": 1}}},
        {"$sort": SON([("theatres_count", -1)])},  # Sort by theatres_count descending
        {"$limit": limit}
    ]
    return list(db.theaters.aggregate(pipeline))


def calculate_distance(coord1, coord2):
    """
    Calculate the distance between two coordinates using the Haversine formula.

    Args:
        coord1 (tuple): The first coordinate (longitude, latitude) in degrees.
        coord2 (tuple): The second coordinate (longitude, latitude) in degrees.

    Returns:
        float: The distance between the two coordinates in kilometers.

    """
    lon1, lat1 = radians(coord1[0]), radians(coord1[1])
    lon2, lat2 = radians(coord2[0]), radians(coord2[1])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    # Radius of the Earth in kilometers
    R = 6371.0

    # Return distance in kilometers
    return R * c


def top_theatres_nearby_coordinates(coordinates, N):
    """
    Find the top N theatres nearby given coordinates.

    Args:
        coordinates (tuple): The coordinates (longitude, latitude) in degrees.
        N (int): The maximum number of theatres to return.

    Returns:
        list: A list of dictionaries containing theatre information.

    """
    theatres = list(db.theaters.find({}))  # Fetch all theatres from the database
    theatres_with_distance = []

    for theatre in theatres:
        theatre_coordinates = theatre['location']['geo']['coordinates']
        distance = calculate_distance(coordinates, theatre_coordinates)
        theatres_with_distance.append((theatre, distance))

    # Sort theatres by distance and limit the result to N
    sorted_theatres = sorted(theatres_with_distance, key=lambda x: x[1])[:N]

    return [theatre[0] for theatre in sorted_theatres]


# Example usage:
print("\nTop 10 cities with the maximum number of theatres:\n")
data1 = top_cities_with_theatres(10)
for entry in data1:
    print(f"{entry['_id']} - {entry['theatres_count']}")

# Example usage
coordinates = (-93.24565, 44.85466)  # Example coordinates
print("\nTop 10 theatres nearby given coordinates:\n")
data2 = top_theatres_nearby_coordinates(coordinates, 10)
for entry in data2:
    print(f"theaterId: {entry['theaterId']}")
