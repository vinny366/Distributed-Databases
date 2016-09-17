package edu.asu.cse512;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.util.GeometryCombiner;


class LConvexHull implements FlatMapFunction<Iterator<String>, Geometry>, Serializable {

	public Iterable<Geometry> call(Iterator<String> lines) {
		List<Coordinate> InputCoordinatePoints = new ArrayList<Coordinate>();
		List<Geometry> LocalCH = new ArrayList<Geometry>();

		Coordinate ct;

		while (lines.hasNext()) {

			String st = lines.next();

			ct = null;

			String[] temp = st.split(",");
			ct = new Coordinate(Double.parseDouble(temp[0]), Double.parseDouble(temp[1]));

			InputCoordinatePoints.add(ct);

		}

		Coordinate[] ctArray = new Coordinate[InputCoordinatePoints.size()];
		InputCoordinatePoints.toArray(ctArray);
		Geometry G = new GeometryFactory().createLineString(ctArray);
		Geometry CH = G.convexHull();

		LocalCH.add(CH);
		// System.out.println(CH.getCoordinates().length);

		return LocalCH;
	}
}

class GConvexHull implements FlatMapFunction<Iterator<Geometry>, String>, Serializable {

	public Iterable<String> call(Iterator<Geometry> localConvexHulls) {
		List<Geometry> InputConvexHullCoordinates = new ArrayList<Geometry>();
		List<Geometry> GlobalConvexHull = new ArrayList<Geometry>();

		while (localConvexHulls.hasNext()) {
			InputConvexHullCoordinates.add(localConvexHulls.next());

		}
		Geometry temp = null;
		temp = GeometryCombiner.combine(InputConvexHullCoordinates);

		GlobalConvexHull.add(temp.convexHull());
		List<String> finalOutput=new ArrayList<String>();
		Coordinate[] finalList=GlobalConvexHull.get(0).getCoordinates();
		
		Set<Coordinate> po=new TreeSet<Coordinate>();
			for(Coordinate g:finalList){
			po.add(g);
		}
		for(Coordinate g:po){
			finalOutput.add(Double.toString(g.x)+","+Double.toString(g.y));
		}
		
		return finalOutput;
	}
}

public class convexHull {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("Group9-ConvexHull");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> rawData = sc.textFile(args[0]);
		JavaRDD<Geometry> localCH = rawData.mapPartitions(new LConvexHull());
		JavaRDD<Geometry> CombLocalCH = localCH.repartition(1);
		JavaRDD<String> globalCH = CombLocalCH.mapPartitions(new GConvexHull());
		Configuration hconf = new Configuration();
		hconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		hconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(URI.create("hdfs://master:54310"), hconf);
			hdfs.delete(new Path(args[1]), true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
				
	    globalCH.saveAsTextFile(args[1]);
		
		

	}


}
