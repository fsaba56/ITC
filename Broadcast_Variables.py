from pyspark import SparkContext

sc = SparkContext("local", "BroadcastExample")

# Creating a small dictionary that we want to broadcast
lookup_dict = {'A': 1, 'B': 2, 'C': 3}

# Broadcasting the dictionary
broadcast_var = sc.broadcast(lookup_dict)

# Example RDD with some data
data = sc.parallelize([('A', 10), ('B', 20), ('C', 30)])

# Accessing the broadcast variable
def lookup_and_add(pair):
    key, value = pair
    lookup_value = broadcast_var.value.get(key, 0)  # Getting the value from the broadcast dictionary
    return (key, value + lookup_value)

# Applying the transformation using the broadcast variable
result = data.map(lookup_and_add)

# Collecting the result
print(result.collect())
