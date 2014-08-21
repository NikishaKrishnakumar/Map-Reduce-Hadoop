
package mapreduce;
 	
import java.io.IOException;
import java.util.*; 	

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
 	
 	public class mapreduce {
 	
	   public static class Map 
	   extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> 
	   {
 	     private final static IntWritable one = new IntWritable(1);
//         private Text word = new Text();
// 	    private String word = new String();
 	    
 	
 	     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) 
 	    		 throws IOException {
 	    	  // Reading the data line by line
 	      String line = value.toString();
 	      //splitting the data in the line separated by a Tab 
 	      String[] tokens = line.split("\t");
 	      
// 	      String manu =tokens[14];
// 	     for (String token: tokens) {
// 	          word.set(token);

 	      // getting the manufacturers details which is at the index 16
 	      String yr = tokens[15];
 	      
 	      if ( yr!= null && yr.length() >= 4) 
/*		  was getting an error of nullpointer exception
           so added a condition to verify the length of year before getting only the YYYY
*/
 	      {	  
 	    	  String yrs = yr.substring(0,4); // Date is in 'YYYYMMDD' format
 	          output.collect(new Text("Year: " +yrs), one);
 	      }
 	     
// 	       StringTokenizer tokenizer = new StringTokenizer(line);
// 	       while (tokenizer.hasMoreTokens()) {
// 	         word.set(tokenizer.nextToken()); 	
// 	         output.collect(word, one);
// 	       }
 	       
 	     }
 	   }
 	
 	   public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
 	     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
 	    		 throws IOException {
 	       int sum = 0;
 	       while (values.hasNext()) {
	         sum += values.next().get(); // Reducer logic, adding the sum
 	       }
 	       output.collect(key, new IntWritable(sum));
 	     }
 	   }
 	
 	   public static void main(String[] args) throws Exception {
 		     // Validating the input parameters
 		   if(args.length!=2)
 		   {
 			System.err.println("Usuage: MapReduce <inputfilename> <outputfilename>");  
 			System.exit(2);
 		   }
 		   
 	     JobConf conf = new JobConf(WordCount.class);
 	     conf.setJobName("Map Reduce");
 	/*
	 Setting the number of mappers and reducers
	*/
 	     conf.setNumMapTasks(1);
 	     conf.setNumReduceTasks(1);
 	     
		  /*
		 Printing the number of mapper and reducers used 
		 */
 	     int a =conf.getNumMapTasks();
 	     int b =conf.getNumReduceTasks();
 	     System.out.println("The number of Mapper Tasks are:" + a);
 	     System.out.println("The number of Reducer Tasks are:" + b);
 	    
 	     conf.setOutputKeyClass(Text.class);
 	     conf.setOutputValueClass(IntWritable.class);
 	
	/*
	setting the mapper and reducer classes
	*/
 	     conf.setMapperClass(Map.class);
 	 //    conf.setCombinerClass(Reduce.class);
 	     conf.setReducerClass(Reduce.class);
 		/*
	setting the input and output classes
	*/
 	     conf.setInputFormat(TextInputFormat.class);
 	     conf.setOutputFormat(TextOutputFormat.class);
 	
 	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
 	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 	
 	     JobClient.runJob(conf);
 	   }
 	}
