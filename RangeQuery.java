package edu.asu.cse512;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

class ReadRec implements Function<String, List<Double>>, Serializable {

	public List<Double> call(String fileLines) throws Exception {

		List<Double> rectanglePoints = new ArrayList<Double>();

		String st = fileLines;
		String[] temp = st.split(",");

		double x1 = Double.parseDouble(temp[0]);
		double y1 = Double.parseDouble(temp[1]);
		double x2 = Double.parseDouble(temp[2]);
		double y2 = Double.parseDouble(temp[3]);

		if (x2 < x1) {
			Double tempo = x1;
			x1 = x2;
			x2 = tempo;
		}

		if (y2 < y1) {
			Double tempo = y1;
			y1 = y2;
			y2 = tempo;
		}

		rectanglePoints.add(x1);
		rectanglePoints.add(y1);
		rectanglePoints.add(x2);
		rectanglePoints.add(y2);

		return rectanglePoints;
	}

}

public class RangeQuery {

	public static void main(String[] args) throws FileNotFoundException {
		try {
			SparkConf conf = new SparkConf().setAppName("Group9-RangeQuery");// .setMaster("spark://192.168.40.10:7077");

			JavaSparkContext sc = new JavaSparkContext(conf);
			// hdfs://master:54310/data/RangeQueryTestData.csv
			// hdfs://master:54310/data/RangeQueryRectangle.csv
			// hdfs://master:54310/output/t
			JavaRDD<String> rawData = sc.textFile(args[0]);
			// JavaRDD<String> rawData =
			// sc.textFile("/home/system/Desktop/files/RangeQueryTestData.csv");
			JavaRDD<String> queryRectangle = sc.textFile(args[1]);// args[1]);
			// JavaRDD<String> queryRectangle =
			// sc.textFile("/home/system/Desktop/files/RangeQueryRectangle.csv");//
			// args[1]);
			JavaRDD<List<Double>> tempo = queryRectangle.map(new ReadRec());

			final Broadcast<List<Double>> broadcastVar = sc.broadcast(tempo.first());

			JavaRDD<String> result = rawData.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {

				public Iterable<String> call(Iterator<String> lines) {
					List<String> LocalPoints = new ArrayList<String>();

					while (lines.hasNext()) {

						String st = lines.next();
						String[] temp = st.split(",");
						if (broadcastVar.value().get(0) <= Double.parseDouble(temp[1])
								&& broadcastVar.value().get(2) >= Double.parseDouble(temp[1])
								&& broadcastVar.value().get(1) <= Double.parseDouble(temp[2])
								&& broadcastVar.value().get(3) >= Double.parseDouble(temp[2])) {

							LocalPoints.add(temp[0]);
						}
					}
					return LocalPoints;
				}

			});

			Configuration hconf = new Configuration();
			hconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
			hconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			FileSystem hdfs = FileSystem.get(URI.create("hdfs://master:54310"), hconf);
			hdfs.delete(new Path(args[2]), true);

			result.saveAsTextFile(args[2]);
			// result.saveAsTextFile("/home/system/Desktop/files/RangeQueryResult.csv");
			sc.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}