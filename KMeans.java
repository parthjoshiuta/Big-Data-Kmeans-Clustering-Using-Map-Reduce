import java.io.*;
import java.util.Scanner;
import java.util.Vector;
import java.util.List;
import java.util.Set;
import java.util.Enumeration;
import java.util.Hashtable;
import java.net.URI; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Point implements WritableComparable<Point> {
    public double x;
    public double y;

    public Point() {}
	public Point(double x, double y)
    {
    	this.x = x; 
    	this.y = y; 
    }
    public String toString()
    {
    return this.x + " " + this.y; 
	}
    //@Override
	public void readFields(DataInput in) throws IOException
	{
		x = in.readDouble();
		y = in.readDouble();		
	}
    //@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeDouble(x);
		out.writeDouble(y);
	}
    //@Override
	public int compareTo(Point j)
	{
		int comp= Double.valueOf(this.x).compareTo(Double.valueOf(j.x)); 
		if (Double.valueOf(this.x).compareTo(Double.valueOf(j.x))== 0)

			{	
				comp = Double.valueOf(this.y).compareTo(Double.valueOf((j.y))); 
			}	
		return comp;
	}
}

class Avg implements Writable {

	public double sumX;
	public double sumY;
	public long count; 
	
	public Avg() {}
	public Avg(double sumX, double sumY, long count)
	{
		this.sumX = sumX;
		this.sumY = sumY;
		this.count = count;	
		 
	}
	 public String toString()
    {
    return this.sumX + " " + this.sumY + " " + this.count;
        }

	public void readFields(DataInput in) throws IOException
        {
                sumX = in.readDouble();
                sumY = in.readDouble();
		count = in.readLong();
        }
    
        public void write(DataOutput out) throws IOException
                    {
                                    out.writeDouble(sumX);
                                    out.writeDouble(sumY);
			out.writeLong(count);
                                                            }
    
}
public class KMeans {
    
    static Vector<Point> centroids = new Vector<Point>(100);
    static Hashtable<Point, Avg> table ; 
    


    public static class AvgMapper extends Mapper<Object,Text,Point,Avg> {
    	@Override
    	protected  void setup(Context context) throws IOException, InterruptedException 
    	{
    		URI[] paths = context.getCacheFiles();
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			BufferedReader r = new BufferedReader(new InputStreamReader(fs.open(new Path(paths[0]))));
	String x = " ";
	try {	
    	while((x = r.readLine())!= null) {
            	String[] y = x.split(",");
            	double p1 = Double.parseDouble(y[0]);
            	double p2 = Double.parseDouble(y[1]);
            	Point p = new Point(p1,p2);
            	centroids.add(p);
		
			}
		}catch(Exception e)
		{
			System.out.println ("Exception");
		}
		//System.out.println(centroids.size());
			r.close();
		table = new Hashtable<Point, Avg>();
		for(Point i:centroids)
			{
			table.put(i, new Avg(0,0,0));
			}
        
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    		
		Set<Point> key2 = table.keySet();
                for(Point k : key2)
                {
                        System.out.println(k.toString() + table.get(k).toString());
			context.write(new Point(k.x, k.y), table.get(k));
                }
		/*

		Set<Point> keys = table.keySet();
		System.out.println(keys); 
		try {
		for(Point c: keys)
		{
		Avg a = table.get(c);
		System.out.println(c.toString() + a.toString());
    		context.write(c, new Avg(a.sumX, a.sumY, a.count));
		}
		}catch(Exception e)
		{
		System.out.println("Exception");
		}   
*/ 
}

    	@Override
    	public void map (Object key,Text line, Context context)
    	{
    		String f = line.toString();
		String[] line_split = f.split(",");
    		double a1 = Double.parseDouble(line_split[0]);
                double a2 = Double.parseDouble(line_split[1]);
    		Point p = new Point(a1, a2);  
    		int count = 0;
    		double min_dist = Double.MAX_VALUE; 
    		double eu_dist = Double.MAX_VALUE ;
    		int min_index = Integer.MAX_VALUE;
		for(Point pq : centroids)
    		{
			//System.out.println(count);
    			eu_dist = Math.sqrt((a1-pq.x)*(a1-pq.x) + (a2-pq.y)*(a2-pq.y));
    			if (eu_dist < min_dist)
    			{
    				min_dist = eu_dist; 
    				min_index = count; 
    			}
    			count= count+1;
    		}
		Point w = centroids.get(min_index);
		Avg b = table.get(w); 
			
		if (b.count == 0)
		{
			table.put(w, new Avg(a1,a2,1));	
		}
		else
		{
			table.put(w, new Avg((b.sumX + a1),( b.sumY + a2), (b.count + 1)));
		}
		/*
		Set<Point> key2 = table.keySet();
		for(Point k : key2)
		{
			System.out.println(k.toString() + table.get(k).toString());
		}
		System.out.println(b.sumX + b.sumY + b.count);
		try
		{
    		context.write(centroids.get(min_index),p);
    		}catch(Exception e)
		{
			System.out.println("Exception");
		}*/
		}

    	}

    

    public static class AvgReducer extends Reducer<Point,Avg,Text,Object> {
	@Override
    	public void reduce (Point c, Iterable<Avg> avgs, Context context)
    	{
    		int count = 0; 
    		double sx = 0.0, sy = 0.0 ; 
    		for (Avg a : avgs)
    		{
    			count += a.count ;  
    			sx += a.sumX ;
    			sy += a.sumY ;
    		}
    		
    		c.x = sx/(double)(count); 
    		c.y = sy/(double)(count);

    		try {	
    		context.write(new Text(c.toString()), NullWritable.get());
 	   	}catch(Exception e)
{
		System.out.println("Exception");
}
	
}
    	}
    

    public static void main ( String[] args ) throws Exception {


    	Job job = Job.getInstance();
        job.setJobName("KmeansJob");
        
        job.setJarByClass(KMeans.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        job.setMapOutputKeyClass(Point.class);
        job.setMapOutputValueClass(Avg.class);
        
        job.setMapperClass(AvgMapper.class);
        
        job.setReducerClass(AvgReducer.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        
        job.addCacheFile(new URI(args[1]));

        job.waitForCompletion(true);
   }
}
