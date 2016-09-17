package edu.asu.cse512;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory; 

class CoordinatePoint implements Comparable<CoordinatePoint>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2114025242525545029L;
	double x;
	double y;

	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}

	public int compareTo(CoordinatePoint p) {
		if (this.x == p.x) {
			return (int) (this.y - p.y);
		} else {
			return (int) (this.x - p.x);
		}
	}

	public String toString() {
		return "(" + x + "," + y + ")";
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof CoordinatePoint) {
			CoordinatePoint toCompare = (CoordinatePoint) o;
			return ((this.x == toCompare.x)) && ((this.y == toCompare.y));
		}
		return false;
	}
}



public class ClosestPair {
	
	public static void sortByXcoordinate(final List<? extends Coordinate> points)
	  {
	    Collections.sort(points, new Comparator<Coordinate>() {
	        public int compare(Coordinate point1, Coordinate point2)
	        {
	          if (point1.x < point2.x)
	            return -1;
	          if (point1.x > point2.x)
	            return 1;
	          if (point1.x == point2.x)
	        	  sortByYcoordinate(points);
	          return 0;
	        }
	      }
	    );
	  }
	
	public static void sortByYcoordinate(List<? extends Coordinate> points)
	  {
	    Collections.sort(points, new Comparator<Coordinate>() {
	        public int compare(Coordinate point1, Coordinate point2)
	        {
	          if (point1.y < point2.y)
	            return -1;
	          if (point1.y > point2.y)
	            return 1;
	          
	          return 0;
	        }
	      }
	    );
	  }

	public static void main(String[] args) throws FileNotFoundException {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("Group9-ClosestPair");//.setMaster("local"); spark://192.168.40.10:7077 [0]);
		JavaSparkContext sc = new JavaSparkContext(conf);
		//JavaRDD<String> rawData = sc.textFile("/home/system/Downloads/TestCase/ClosestPairTestData.csv");// args[1]);
		JavaRDD<String> rawData = sc.textFile(args[0]);
		JavaRDD<Coordinate> localPartitions = rawData
				.mapPartitions(new FlatMapFunction<Iterator<String>, Coordinate>() {

					private static final long serialVersionUID = 798272118013563992L;

//					@Override
					public Iterable<Coordinate> call(Iterator<String> t) throws Exception {
						List<CoordinatePoint> points = new ArrayList<CoordinatePoint>();
						List<Coordinate> geoCoords = new ArrayList<Coordinate>();
						while (t.hasNext()) {
							String[] temp = (t.next()).split(",");
							CoordinatePoint point1 = new CoordinatePoint();
//							System.out.println(temp[0] +"--,--"+ temp[1]);
							point1.setX(Double.parseDouble(temp[0]));
							point1.setY(Double.parseDouble(temp[1]));
							points.add(point1);
							Coordinate coord = new Coordinate();
							coord.x = point1.getX();
							coord.y = point1.getY();
							geoCoords.add(coord);
						}
						GeometryFactory geometry = new GeometryFactory();
					
						ConvexHull local = new ConvexHull(geoCoords.toArray(new Coordinate[geoCoords.size()]),
								geometry);
						Geometry localGeom = local.getConvexHull();
						List<Coordinate> localConvexHullPoints = Arrays.asList(localGeom.getCoordinates());
						Coordinate[] localClosestPair = findClosestPair(geoCoords);
						
						List<Coordinate> finalLocalConvexPoints = new ArrayList<Coordinate>();

						int siz = localConvexHullPoints.size();
						for (int i = 0; i < siz - 1; i++) {
							Coordinate c = localConvexHullPoints.get(i);
							finalLocalConvexPoints.add(c);
						}
					
						if (!localConvexHullPoints
								.contains(new Coordinate(localClosestPair[0].x, localClosestPair[0].y)))
							finalLocalConvexPoints.add(localClosestPair[0]);
						if (!localConvexHullPoints
								.contains(new Coordinate(localClosestPair[1].x, localClosestPair[1].y)))
							finalLocalConvexPoints.add(localClosestPair[1]);

						return finalLocalConvexPoints;
					}

					private Coordinate[] findClosestPair(List<Coordinate> geoCoords) {
					
						Coordinate[] coordinates = new Coordinate[geoCoords.size()];
						Coordinate[] closestCoordinate = new Coordinate[2];
						for (int j = 0; j < geoCoords.size(); j++) {
							coordinates[j] = geoCoords.get(j);
						}
						Geometry g1 = new GeometryFactory().createLineString(coordinates);
						Coordinate[] min = g1.getCoordinates();
						double minDistance = 999999999;
			
						for (Coordinate c1 : min) {
						
							for (Coordinate c2 : min) {

								// System.out.println(c1.x +"---!"+ c2.y);
								if (c1 == c2) {
								} else {
									// System.out.println(c1.x + "c1y"+c1.y
									// +"c2x" +c2.x +" c2"+ c2.y);
									double thisDistance = findDistance(c1.x, c1.y, c2.x, c2.y);
									if (thisDistance < minDistance) {
										minDistance = thisDistance;
										closestCoordinate[0] = c1;
										closestCoordinate[1] = c2;
									}
									else if(thisDistance == minDistance){
										
									}
								} // else close
							}
						}
						
						return closestCoordinate;
					}

					private double findDistance(double x, double y, double x2, double y2) {
						double dx = x - x2;
						double dy = y - y2;
						return Math.sqrt(dx * dx + dy * dy);
					}

				});

		JavaRDD<Coordinate> ReduceList = localPartitions.repartition(1);
		JavaRDD<String> FinalList = ReduceList
				.mapPartitions(new FlatMapFunction<Iterator<Coordinate>, String>() {

					private static final long serialVersionUID = -2155848408503019739L;

//					@Override
					public Iterable<String> call(Iterator<Coordinate> t) throws Exception {
						
						List<Coordinate> finalPoints = new ArrayList<Coordinate>();
						while (t.hasNext()) {
							finalPoints.add(t.next());
							
						}
						//Coordinate[] finalClosestPair = findClosestPair(finalPoints);
						List<Coordinate[]> finalClosestPair=findClosestPair(finalPoints);
						List<String> finalCoordinates=new ArrayList<String>();
						int n=finalClosestPair.size();
						int counter=0;
						for(Coordinate[] c: finalClosestPair){
							if(counter<(n/2))
							{
								finalCoordinates.add(Double.toString(c[0].x)+","+Double.toString(c[0].y));
								finalCoordinates.add(Double.toString(c[1].x)+","+Double.toString(c[1].y));
							}
							counter++;
						}
						return finalCoordinates;
					}

					
					private List<Coordinate[]> findClosestPair(List<Coordinate> finalPoints) {
						Coordinate[] coordinates = new Coordinate[finalPoints.size()];
						List<Coordinate[]> closestCoordinatesList=new ArrayList<Coordinate[]>();
						
						for (int j = 0; j < finalPoints.size(); j++) {
							coordinates[j] = finalPoints.get(j);
						}
						for(int j=0;j<finalPoints.size();j++){
							//System.out.println(coordinates[j]);
						}
						double minDistance = 999999999;
						for (Coordinate c1 : coordinates) {

							for (Coordinate c2 : coordinates) {
									
								//System.out.println(c1.x +"---!"+ c2.y);
								if (c1 == c2) {
								} else {
									// System.out.println(c1.x + "c1y"+c1.y
									// +"c2x" +c2.x +" c2"+ c2.y);
									double thisDistance = findDistance(c1.x, c1.y, c2.x, c2.y);
									if (thisDistance < minDistance) {
										closestCoordinatesList.clear();
										minDistance = thisDistance;
										Coordinate[] closestCoordinate = new Coordinate[2];
										closestCoordinate[0] = c1;
										closestCoordinate[1] = c2;
										closestCoordinatesList.add(closestCoordinate);
									}
									else if(thisDistance == minDistance){
										Coordinate[] closestCoordinate = new Coordinate[2];
										closestCoordinate[0] = c1;
										closestCoordinate[1] = c2;
										closestCoordinatesList.add(closestCoordinate);
									}
								}
							}
						}
						List<Coordinate[]> results=new ArrayList<Coordinate[]>();
						
						for(Coordinate[] c:closestCoordinatesList){
							Set<Coordinate> sort=new TreeSet<Coordinate>();
							sort.add(c[0]);
							sort.add(c[1]);
							Iterator ite=sort.iterator();
							Coordinate[] c1=new Coordinate[2];
							int cl=0;
							while(ite.hasNext()){
								c1[cl++]=(Coordinate) ite.next();
							}
							results.add(c1);
						}
						
						return results;
					}

					private double findDistance(double x, double y, double x2, double y2) {
						double dx = x - x2;
						double dy = y - y2;
						return Math.sqrt(dx * dx + dy * dy);
					}
				});
		// send points
		//ClosestPairRDD.sortByXcoordinate(FinalList);
		//
		//FinalList.saveAsTextFile("/home/system/Downloads/outputnew");
		try{
			Configuration hconf = new Configuration();
            hconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            hconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            FileSystem hdfs;
            hdfs = FileSystem.get(URI.create("hdfs://master:54310"), hconf);

            hdfs.delete(new Path(args[1]), true);          
           
            FinalList.saveAsTextFile(args[1]);

            sc.stop();
    		sc.close();

		}
		catch(Exception e){
			  e.printStackTrace();
		}		
		
	}

}