package edu.asu.cse512;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;


class Rectangle
{	
	public static Polygon computeRectCoord(ArrayList<Double> tempCoord)
	{ 
		Double x1 = tempCoord.get(0);
		Double y1 = tempCoord.get(1);
		Double x2 = tempCoord.get(2);
		Double y2 = tempCoord.get(3);
		GeometryFactory geom = new GeometryFactory();
		Polygon poly = geom.createPolygon(new Coordinate[]{new Coordinate(x1,y1),
			    new Coordinate(x1, y2),
				new Coordinate(x2, y2),
				new Coordinate(x2, y1),
				new Coordinate(x1, y1)});
	
		return poly;
	}
	
}

class LPolyUnion extends Rectangle implements FlatMapFunction<Iterator<String>, Polygon>, Serializable
{
	
	public Iterable<Polygon> call(Iterator<String> lines)
	{	
		List<Polygon> InputPolygons = new ArrayList<Polygon>();
		List<Polygon> LUPolygons = new ArrayList<Polygon>();
		
		GeometryFactory geom = new GeometryFactory();
		
		while(lines.hasNext())
		{	
			
			String st = lines.next();
			
			ArrayList<Double> rectCoord = new ArrayList<Double>(); 
			for (String rt: st.split(","))
			{
				rectCoord.add(Double.parseDouble(rt));		
			}
			
			InputPolygons.add(Rectangle.computeRectCoord(rectCoord));
			
		}
		Geometry localGm = CascadedPolygonUnion.union(InputPolygons);
		
		for(int i = 0; i < localGm.getNumGeometries() ; i++)
		{
			Polygon temp = geom.createPolygon(localGm.getGeometryN(i).getCoordinates());
			LUPolygons.add(temp);
		}
		
		return LUPolygons;
	}
}
class GPolyUnion implements FlatMapFunction<Iterator<Polygon>, Coordinate>, Serializable
{
	
	public Iterable<Coordinate> call(Iterator<Polygon> localPolys)
	{	
		List<Polygon> InputPolygons = new ArrayList<Polygon>();
		Set<Coordinate> GUPolygons = new TreeSet<Coordinate>();
		
		GeometryFactory geom = new GeometryFactory();
		
		while(localPolys.hasNext())
		{	
			InputPolygons.add(localPolys.next());
		}
		Geometry globalGm = CascadedPolygonUnion.union(InputPolygons);
		
		for(int i = 0; i < globalGm.getNumGeometries() ; i++)
		{
			Coordinate[] temp = globalGm.getGeometryN(i).getCoordinates();
			for (Coordinate ct: temp)
				GUPolygons.add(ct);
			
		}
		
		return GUPolygons;
	}
}
public class Union {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("Group9-Union");//"spark://192.168.0.90:7077");
		JavaSparkContext sc = new JavaSparkContext(conf);		
		JavaRDD<String> rawData = sc.textFile(args[0]);
		JavaRDD<Polygon> localPU = rawData.mapPartitions(new LPolyUnion());
		JavaRDD<Polygon> CombLocalUnion = localPU.repartition(1);
		JavaRDD<Coordinate> globalPU = CombLocalUnion.mapPartitions(new GPolyUnion());
		JavaRDD<String> mapj=globalPU.map(new Function<Coordinate,String>()
				{

					public String call(Coordinate arg0) throws Exception {
						// TODO Auto-generated method stub
						String pq=arg0.toString();
					    String[] ge =pq.split(",");
					    String t=ge[0].replace("(", "")+","+ge[1].replace(" ", "");
						return t;
					}
			
				});
		
		Configuration hconf = new Configuration();
		hconf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
		hconf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(URI.create("hdfs://master:54310"), hconf);
			hdfs.delete(new Path(args[1]), true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		mapj.saveAsTextFile(args[1]);
	}
	

}
