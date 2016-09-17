package operation7;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import edu.asu.cse512.fall.joinQuery.Join;

public class aggregation {

	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf().setAppName("Group9-SpatialAggregation");
		//SparkConf conf = new SparkConf().setAppName("App").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.addJar("/home/system/Downloads/Group9/joinQuery-0.1.jar");
		Join join=new Join();
		join.main(args);
		JavaRDD<String> inputData1 = sc.textFile(args[2]);
		JavaRDD<String> output=inputData1.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {

			public Iterable<String> call(Iterator<String> t) throws Exception {
				List<String> recordList=new ArrayList<String>();
				while(t.hasNext()){
					String[] temp=t.next().split(",");
					String outputRecord=temp[0]+","+Integer.toString(temp.length-1);
					recordList.add(outputRecord);
				}
				return recordList;
			}
			
		}).repartition(1);
		output.saveAsTextFile(args[3]);
		sc.stop();
		sc.close();
	}
}

