package missingpokercards;
import java.io.File;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class missingpokercards {
	
	public static class mapping extends Mapper<LongWritable, Text, Text, IntWritable>{
		  
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	    {	
	        String readLines = value.toString();
	        String[] lineSplit = readLines.split("\t");

	      	context.write(new Text(lineSplit[0]), new IntWritable(Integer.parseInt(lineSplit[1])));
	    }
	}

	public static class reducing extends Reducer<Text, IntWritable, Text, IntWritable>{   
	    public void reduce(Text key, Iterable<IntWritable> cardVal, Context context)
	    throws IOException, InterruptedException {
	    	
	    	System.out.println(key);
	    	ArrayList<Integer> nums = new ArrayList<Integer>();
	    	for (IntWritable val : cardVal) {
	    		nums.add(val.get());
	    	}
	   
	    	for (int i = 1;i <= 13;i++){
	    		if(nums.contains(i)) {
	    			continue;
	    		}
	    		context.write(key, new IntWritable(i));
	    	}

	    }    
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    @SuppressWarnings("deprecation")
		Job job = new Job(conf, "Find missing Cards");
	    job.setJarByClass(missingpokercards.class);
	    job.setMapperClass(mapping.class);
	    job.setReducerClass(reducing.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
    	File directory = new File(args[1]);

        try{
     	   
            delete(directory);
     	
        }catch(IOException e){
            e.printStackTrace();
            System.exit(0);
        }
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void delete(File file)
	    	throws IOException{
	 
	    	if(file.isDirectory()){
	 
	    		//directory is empty, then delete it
	    		if(file.list().length==0){
	    			
	    		   file.delete();
	    		   System.out.println("Directory is deleted : " 
	                                                 + file.getAbsolutePath());
	    			
	    		}else{
	    			
	    		   //list all the directory contents
	        	   String files[] = file.list();
	     
	        	   for (String temp : files) {
	        	      //construct the file structure
	        	      File fileDelete = new File(file, temp);
	        		 
	        	      //recursive delete
	        	     delete(fileDelete);
	        	   }
	        		
	        	   //check the directory again, if empty then delete it
	        	   if(file.list().length==0){
	           	     file.delete();
	        	     System.out.println("Directory is deleted : " 
	                                                  + file.getAbsolutePath());
	        	   }
	    		}
	    		
	    	}else{
	    		//if file, then delete it
	    		file.delete();
	    		System.out.println("File is deleted : " + file.getAbsolutePath());
	    	}
	    }

}
