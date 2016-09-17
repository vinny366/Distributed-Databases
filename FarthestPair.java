package edu.asu.cse512;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
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

		return LocalCH;
	}
}

class GConvexHullFP implements FlatMapFunction<Iterator<Geometry>, String>, Serializable {

	public Iterable<String> call(Iterator<Geometry> localConvexHulls) {
		List<Geometry> InputConvexHullCoordinates = new ArrayList<Geometry>();

		while (localConvexHulls.hasNext()) {
			InputConvexHullCoordinates.add(localConvexHulls.next());

		}
		Geometry temp = null;
		temp = GeometryCombiner.combine(InputConvexHullCoordinates);

		temp = temp.convexHull();

		Coordinate[] coh = temp.getCoordinates();
		coh = (Coordinate[]) ArrayUtils.remove(coh, coh.length - 1);

		double maxDistance = 0;
		List<Coordinate[]> farthestCoordinate = new ArrayList<Coordinate[]>();

		for (Coordinate k1 : coh) {
			for (Coordinate k2 : coh) {
				if (k1 == k2) {
				} else {
					double cDist = Distance.getDistance(k1.x, k1.y, k2.x, k2.y);
					if (cDist >= maxDistance) {
						if (cDist == maxDistance) {
							Coordinate[] k = new Coordinate[2];
							k[0] = k1;
							k[1] = k2;
							if (!farthestCoordinate.contains(new Coordinate(k[0].x, k[0].y))
									&& !farthestCoordinate.contains(new Coordinate(k[1].x, k[1].y)))
								farthestCoordinate.add(k);

						}

						else {
							farthestCoordinate.clear();
							maxDistance = cDist;
							Coordinate[] k = new Coordinate[2];
							k[0] = k1;
							k[1] = k2;
							if (!farthestCoordinate.contains(new Coordinate(k[0].x, k[0].y))
									&& !farthestCoordinate.contains(new Coordinate(k[1].x, k[1].y)))
								farthestCoordinate.add(k);
						}

					}
				}
			}
		}

		List<Geometry> farthestGeometry = new ArrayList<Geometry>();
		// System.out.println(farthestCoordinate);

		List<Coordinate[]> finalOutputs = new ArrayList<Coordinate[]>();
		for (Coordinate[] c : farthestCoordinate)

		{
			if (c[0].x < c[1].x) {
				finalOutputs.add(c);
			}
		}

		for (Coordinate[] Gt : finalOutputs)

		{
			Geometry F = new GeometryFactory().createLineString(Gt);
			farthestGeometry.add(F);
		}
		List<String> finalOutputStrings = new ArrayList<String>();
		for (Coordinate[] Gt : finalOutputs) {
			finalOutputStrings.add(Double.toString(Gt[0].x) + "," + Double.toString(Gt[0].y));
			finalOutputStrings.add(Double.toString(Gt[1].x) + "," + Double.toString(Gt[1].y));
		}

		return finalOutputStrings;
	}
}

public class FarthestPair {

	public static void main(String[] args) {
		try {
			SparkConf conf = new SparkConf().setAppName("Group9-FarthestPair");
			JavaSparkContext sc = new JavaSparkContext(conf);
			JavaRDD<String> rawData = sc.textFile(args[0]);
			//JavaRDD<String> rawData = sc.textFile("/home/system/Downloads/Test Case (2)/FarthestPairTestData.csv",2);
			JavaRDD<Geometry> localCH = rawData.mapPartitions(new LConvexHull());
			JavaRDD<Geometry> CombLocalCH = localCH.repartition(1);
			JavaRDD<String> globalCH = CombLocalCH.mapPartitions(new GConvexHullFP());

			Configuration hconf = new Configuration();
			hconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
			hconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			FileSystem hdfs;
			hdfs = FileSystem.get(URI.create("hdfs://master:54310"), hconf);

			hdfs.delete(new Path(args[1]), true);
						
			globalCH.saveAsTextFile(args[1]);

			sc.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/*
	 * public static boolean deleteDir(File dir) { if (dir.isDirectory()) {
	 * String[] children = dir.list(); for (int i = 0; i < children.length; i++)
	 * { boolean success = deleteDir(new File(dir, children[i])); if (!success)
	 * { return false; } } } return dir.delete(); }
	 */

}

class Distance {

	public static double getDistance(double x1, double y1, double x2, double y2) {
		double dx = x1 - x2;
		double dy = y1 - y2;

		return Math.sqrt(dx * dx + dy * dy);
	}

}