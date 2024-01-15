from pyspark import SparkContext
from itertools import combinations
import sys
import math
import time
from itertools import combinations

def print_results(results,type):
    # Initialize the current size and the output string
    current_size = 1
    output = f"{type}\n"

    # Iterate over the results
    for itemset, count in results:
        # If the itemset is a singleton, convert it to a tuple
        if not isinstance(itemset, tuple):
            itemset = (itemset,)

        # If the size of the current itemset is different from the current size, add a newline to the output
        if len(itemset) != current_size:
            output = output[:-1] + "\n\n"  # Remove the trailing comma from the previous line
            current_size = len(itemset)

        # Add the current itemset to the output
        if len(itemset) == 1:
            output += "('" + itemset[0] + "'),"
        else:
            output += str(itemset) + ","

    # Remove the trailing comma from the output
    output = output[:-1]

    # Print the output
    print(output)

def write_results(results, type, file):
    # Initialize the current size and the output string
    current_size = 1
    output = f"{type}\n"

    # Iterate over the results
    for itemset, count in results:
        # If the itemset is a singleton, convert it to a tuple
        if not isinstance(itemset, tuple):
            itemset = (itemset,)

        # If the size of the current itemset is different from the current size, add a newline to the output
        if len(itemset) != current_size:
            output = output[:-1] + "\n\n"  # Remove the trailing comma from the previous line
            current_size = len(itemset)

        # Add the current itemset to the output
        if len(itemset) == 1:
            output += "('" + itemset[0] + "'),"
        else:
            output += str(itemset) + ","

    # Remove the trailing comma from the output
    output = output[:-1]
    # putting a spce between the two
    if type == 'Frequent Itemsets:':
        output = "\n\n" + output

    # Write the output to the file
    file.write(output)

def process_data(rdd):
    # Split the data
    rdd = rdd.map(lambda line: [x.replace('"', '') for x in line.split(",")])    

    # Remove the header
    header = rdd.first()
    rdd = rdd.filter(lambda line: line != header)

    # Select TRANSACTION_DT, CUSTOMER_ID, PRODUCT_ID and rename header 
    rdd = rdd.map(lambda item: (item[0], int(item[1]), int(item[5]))).map(lambda header: (header[0] + "-" + str(header[1]), str(header[2])))

    # Collect the data to the driver
    rdd_data = rdd.collect()

    return rdd_data 


def preprocessing(file,filter):
    # reading in the data to sere if its case 1 or case 2
    text_rdd = sc.textFile(file)
    text_rdd_header = text_rdd.first()
    data_rdd = text_rdd.filter(lambda row: row != text_rdd_header)
    rdd = data_rdd.map(lambda row: (row.split(',')[0], row.split(',')[1])).groupByKey().mapValues(set).map(lambda item: sorted(list(item[1])))
    rdd = rdd.filter(lambda x: len(x) > filter)
    return rdd

def counting_baskets(rdd):
    count = rdd.count()
    return count

def PCY(iterator, support, n_baskets):

    # Convert iterator to list
    partition = list(iterator)

    # Calculate for each basket given p*s
    p = len(partition) / n_baskets
    s = int(support)
    support_thres= math.ceil(p*s)

    singletons = {}
    num_buckets = len(partition)
    bucketCounts = {} 


    # First pass
    for basket in partition:
        for item in basket:
            if item not in singletons:
                singletons[item] = 0
            singletons[item] += 1
        
        for pair in combinations(basket, 2):
            bucket = hash(pair) % num_buckets
            if bucket not in bucketCounts:
                bucketCounts[bucket] = 0
            bucketCounts[bucket] += 1

    # Getting the frequent pairs and making sure they are actually frequent
    single_frequent = set()
    for item, count in singletons.items():
        if count >= support_thres:
            single_frequent.add(item)


    # Create bitmap and getting the frequent bitmaps
    bitmap = [0] * num_buckets
    for bucket, count in bucketCounts.items():
        if count >= support_thres:
            bitmap[bucket] = 1

    candidate_pairs = {}

    # Second pass
    for basket in partition:
        for pair in combinations(basket, 2):
            sorted_pair = tuple(sorted(pair))
            # if the singletons are frequent 
            if set(sorted_pair).issubset(single_frequent):
                bucket = hash(sorted_pair) % num_buckets
                # if bitmap is 1 
                if bitmap[bucket] == 1:
                    if sorted_pair not in candidate_pairs:
                        candidate_pairs[sorted_pair] = 0
                    candidate_pairs[sorted_pair] += 1


    # Getting the frequent singletons
    pairs_frequent = set()
    for item, count in candidate_pairs.items():
        if count >= support_thres:
            pairs_frequent.add(item)

    # frequent singletons
    for item in single_frequent:
        yield (item, singletons[item])
    # frequent pairs 
    for pair in pairs_frequent:
        yield (pair, candidate_pairs[pair])

    # Generate larger frequent itemsets
    frequent_items= pairs_frequent
    k = 3

    while True:
        candidate_itemsets = set()
        unique_items = set(a for b in frequent_items for a in b)


        # getting the canidates
        for itemset in combinations(unique_items, k):
            sorted_itemset = tuple(sorted(itemset))
            if sorted_itemset not in candidate_itemsets and all(subset in frequent_items for subset in combinations(sorted_itemset, k-1)):
                candidate_itemsets.add(sorted_itemset)
        # pruning the canidates 
        frequent_items = set()
        for itemset in candidate_itemsets:
            sorted_itemset = tuple(sorted(itemset))
            count = sum(1 for basket in partition if set(sorted_itemset).issubset(basket))
            if count >= support_thres:
                frequent_items.add(sorted_itemset)
                yield (sorted_itemset, count)
        
        if not frequent_items:
            break
        
        k += 1






def count_itemsets(iterator, candidates):
    partition = list(iterator)
    itemset_counts = {}
    for item in candidates:
        # If the itemset is a singleton, convert it to a tuple
        if not isinstance(item, tuple):
            item = (item,)
        for basket in partition:
            if set(item).issubset(set(basket)):
                if item in itemset_counts:
                    itemset_counts[item] += 1
                else:
                    itemset_counts[item] = 1
    for item, count in itemset_counts.items():
        yield (item, count)

def son_pass_1(rdd, support, n_baskets):
    # First pass of SON algorithm  -- use reducer to get only the distinct items
    candidate_itemsets = rdd.mapPartitions(lambda iterator: PCY(iterator, support, n_baskets)).reduceByKey(lambda x, y: x + y)

    can_results = candidate_itemsets.collect()
    # Sort the results lexicographically
    can_results.sort(key=lambda x: (len(x[0]) if isinstance(x[0], tuple) else 1, x[0] if isinstance(x[0], tuple) else (x[0],)))
    candidates = [x[0] for x in can_results]
    # Print the results
    # print_results(can_results,'Candidates:')
    return  candidates, can_results

def son_pass_2(rdd, candidates, support):
    son_passtwo = rdd.mapPartitions(lambda iterator: count_itemsets(iterator, candidates))
    # Get the full itemsets
    son_passtwo = son_passtwo.reduceByKey(lambda x, y: x + y)

    # make sure they are actually frequent 
    frequent_itemsets = son_passtwo.filter(lambda x: x[1] >= support)
    results = frequent_itemsets.collect()
    # Sort the results lexicographically
    results.sort(key=lambda x: (len(x[0]) if isinstance(x[0], tuple) else 1, x[0] if isinstance(x[0], tuple) else (x[0],)))
    return results

if __name__ == '__main__':
        # Load the data
    time_start= time.time()
    input_filepath = sys.argv[3]
    intermediate_filepath = './result_inter.csv'
    output_filepath = sys.argv[4]
    sc= SparkContext('local[*]','task2') 
    rdd = sc.textFile(input_filepath)
    intermediate_results = process_data(rdd)
    support =  float(sys.argv[2])
    filter = float(sys.argv[1])


    # Wrie the data to a CSV file
    with open(intermediate_filepath, 'w') as f:
        f.write("DATE-CUSTOMER_ID,PRODUCT_ID\n")
        for line in intermediate_results:
            f.write(','.join(line) + "\n")

    rdd = preprocessing(intermediate_filepath,filter)
    n_baskets = counting_baskets(rdd)

    
     # SON ALGORITHM IMPLEMENTATION

    # PHASE 1:
    #  Map -- Ouputs every frequent canidate from each bucket
    #    Ouputs every frequent canidate from each bucket
    #  Reduce -- This gets rid of all the different types of pairs and only keeps the frequent ones 

   
    # first pass of SON algorithm
    candidates, can_results  = son_pass_1(rdd, support, n_baskets)

    # Second pass of son algorithm just checks if the canidates are actually frequent 
    # second pass of son algorithm
    results = son_pass_2(rdd, candidates, support)

    # Print the results
    # print_results(results,'Frequent Itemsets:')
    with open(output_filepath, 'w') as f:
        # Write the candidates
        write_results(can_results, 'Candidates:', f)
        # Write the frequent itemsets
        write_results(results, 'Frequent Itemsets:', f)
    time_end = time.time()
    print('Duration:', time_end-time_start)


    