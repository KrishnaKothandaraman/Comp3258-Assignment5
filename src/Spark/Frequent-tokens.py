from pyspark import SparkConf, SparkContext
import argparse
import os

def valid_file_path(file_path):
    if not os.path.isfile(file_path):
        raise argparse.ArgumentTypeError(f"Invalid file path: {file_path}")
    return file_path


def perform_task2(input_file_path):
	conf = SparkConf().setAppName("Rank_Click_Urls")
	cluster = SparkContext(conf=conf)
	rdd = cluster.textFile(input_file_path)
	urls = rdd.map(lambda line: line.split("\t")[2])
	tokens = urls.flatMap(lambda domain: domain.split("."))
	map_reduce = tokens.filter(lambda token: token != "").map(
	    lambda token: (token, 1)).reduceByKey(lambda a, b: a + b)

	for (token, count) in top10:
	    print(f"({token}, {count})")


	cluster.stop()



if __name__ == "__main__":
	parser = argparse.ArgumentParser("Spark Processing Tasks!!")
	parser.add_argument('-t', required=True, choices=['2', '3'], help="Specify task. Either `1` or `2`")
	parser.add_argument('-i', required=True, type=valid_file_path, help="Specify path to input file")

	args = parser.parse_args()
	task = args.t
	input_file = args.i

	if task == '2':
		perform_task2(input_file)
	else:
		print("ERR")



