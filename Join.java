package edu.asu.cse512;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class Join {
	public static void main(String args[]){
	
		SparkConf conf = new SparkConf().setAppName("App").setMaster("local");
		//SparkConf conf = new SparkConf().setAppName("Group9-SpatialJoin");
		JavaSparkContext sc = new JavaSparkContext(conf);
		try{
			JavaRDD<String> inputData1 = sc.textFile("/home/system/Downloads/TestCase(2)/JoinQueryInput2.csv");
			JavaRDD<String> inputData2 = sc.textFile("/home/system/Downloads/TestCase(2)/JoinQueryInput3.csv");
			//JavaRDD<String> inputData1 = sc.textFile(args[0]);
			//JavaRDD<String> inputData2 = sc.textFile(args[1]);
			List<String> s=inputData1.collect();
			Broadcast<List<String>> broadcast=sc.broadcast(s);
			final List<String> inputDataBroad=broadcast.value();
			
			JavaRDD<String> output=inputData2.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 8202534097953997720L;

				public Iterable<String> call(Iterator<String> t)
						throws Exception {
					Map<Integer,String> output=new TreeMap<Integer,String>();
					while(t.hasNext()){
						String[] temp=(t.next()).split(",");
						Set<Integer> results=new TreeSet<Integer>();
						String intersection=new String();
						for(String s:inputDataBroad){
							String temp2[]=s.split(",");
							if(temp.length==5){
								Geometry g1 = convertToGeometry(temp);
								Geometry g2=convertToGeometry(temp2);
								if(g1.intersects(g2)){
									results.add(Integer.parseInt(temp2[0]));
								}
							}
							else if(temp.length==3){
								double x1=Double.parseDouble(temp2[1]);
								double y1=Double.parseDouble(temp2[2]);
								double x2=Double.parseDouble(temp2[3]);
								double y2=Double.parseDouble(temp2[4]);
								
								double a1=Double.parseDouble(temp[1]);
								double b1=Double.parseDouble(temp[2]);
								
								if((a1 < Math.max(x1, x2)) && (b1 < Math.max(y1, y2))&&(a1 > Math.min(x1, x2))&&(b1 > Math.min(y1, y2)))
								{
									results.add(Integer.parseInt(temp2[0]));
								}
							}
						}
						for(int d:results){
							intersection=intersection+Integer.toString(d)+",";
						}
						output.put(Integer.parseInt(temp[0]), intersection);
					}
					List<String> results=new ArrayList<String>();
					for(Map.Entry<Integer, String> entry : output.entrySet()){ 
						results.add(Integer.toString(entry.getKey())+","+entry.getValue().substring(0, entry.getValue().length()-1));
					}
					
					return results;
				}

				private Geometry convertToGeometry(String[] temp) {
					double x1 = Double.parseDouble(temp[1]);
					double y1 = Double.parseDouble(temp[2]);
					double x2 = Double.parseDouble(temp[3]);
					double y2 = Double.parseDouble(temp[4]);
					
					GeometryFactory geom = new GeometryFactory();
					Geometry poly = geom.createPolygon(new Coordinate[]{new Coordinate(x1,y1),
						    new Coordinate(x1, y2),
							new Coordinate(x2, y2),
							new Coordinate(x2, y1),
							new Coordinate(x1, y1)});
					return poly;
				}
				
			}).repartition(1);
		
			File finalDir = new File("/home/system/DDS/finalOutput");
//			Configuration hconf = new Configuration();
//			hconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//			hconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//			FileSystem hdfs = FileSystem.get(URI.create("hdfs://master:54310"), hconf);
//			hdfs.delete(new Path(args[2]), true);
			List<String> finalOutput=output.collect();
			Map<Integer,String> finalHashResults=new TreeMap<Integer,String>();
			for(String s1:finalOutput){
				int i=s1.indexOf(",");
				finalHashResults.put(Integer.parseInt(s1.substring(0, i)), s1.substring(i, s1.length()));
			}
			List<String> results=new ArrayList<String>();
			for(Map.Entry<Integer, String> entry : finalHashResults.entrySet()){ 
				results.add(Integer.toString(entry.getKey())+entry.getValue());
			}
			JavaRDD<String> finalResults=sc.parallelize(results);
			finalResults.saveAsTextFile(finalDir.toString());
			//finalResults.saveAsTextFile(args[2]);
			sc.stop();
			sc.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	}
}
