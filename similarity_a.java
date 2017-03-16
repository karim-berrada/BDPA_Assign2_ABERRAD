package mypackage.similarity_a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.List;

import java.util.ArrayList;



public class similarity_a extends Configured implements Tool {

   
   public static enum COUNTER {
      COMP_COUNT
      };
           
   public static void main(String[] args) throws Exception {
     
      System.out.println(Arrays.toString(args));      
      
      int res = ToolRunner.run(new Configuration(), new similarity_a(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      
      Job job = new Job(getConf(), "similarity_a");
      // Getting the jobs ready
      job.setJarByClass(similarity_a.class);
      
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      // Mapper and Reducer 
      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(KeyValueTextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      // We define the separator as "," 
      job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
      
      // Define the output as csv 
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);

      // Writing
      FileSystem fs = FileSystem.newInstance(getConf());
      long counter = job.getCounters().findCounter(COUNTER.COMP_COUNT).getValue();
      
      Path outFile = new Path(new Path(args[1]),"number_of_similarity_a.txt");
      
      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(outFile, true)));
      writer.write(String.valueOf(counter));
      writer.close();
      
      return 0;
   }
   
   public static class Map extends Mapper<Text, Text,Text,Text> {

     // Hashmap to store key and the word   
      private static HashMap<String,String> aux_hashmap = new HashMap<String,String>();
     
      
      @Override
      public void map(Text key, Text value, Context context)
              throws IOException, InterruptedException {

          // Reading the file 
          BufferedReader Reader_count = new BufferedReader(new FileReader(new File("/home/Documents/workspace/assignement_2/sample_preprocessed.txt")));
             
             String row;

             while ((row = Reader_count.readLine()) != null){
                 String[] parts = row.split(",", 2);
                 // Getting rid of empty
                 if (parts.length < 2){
               	  System.out.println("Row not taken");
                 } 
                 else {
               	  aux_hashmap.put(parts[0],parts[1]);
                 }
             }
             
             Reader_count.close();
             
              if (!value.toString().isEmpty()){

	              for (String document : aux_hashmap.keySet()) {
	               
	                  // This is comparing ids to not have duplicates
	                  
	                  int _key = Integer.valueOf(key.toString());
	                  int _document = Integer.valueOf(document);
	                                    
	                    if (_document > _key ){
	                  
		                    StringBuilder key_aux = new StringBuilder();
		                    key_aux.append("[");
		                    key_aux.append(key.toString());
		                    
		                    key_aux.append("--");
		                    
		                    key_aux.append(document);
		                    
		                    key_aux.append("]");
		                    
		                    context.write(new Text(key_aux.toString()), new Text(value.toString()));
		                    
		                    }
	                    }
	              }
              }
      }
   
   
   public static class Reduce extends Reducer<Text, Text, Text, Text> {

      private static HashMap<String,String> document_id = new HashMap<String,String>();
   
      public Reduce() throws NumberFormatException, IOException{
 
       BufferedReader Reader_count = new BufferedReader(new FileReader(new File("/home/Documents/workspace/assignement_2/sample_preprocessed.txt")));
          
          String row;

          while ((row = Reader_count.readLine()) != null){
              String[] parts = row.split(",", 2);
              if (parts.length < 2){
            	  System.out.println("Row not taken ");
              
              } else {
            	  document_id.put(parts[0],parts[1]);
              }
          }
          Reader_count.close();
       
        }

      	// This is the function that computes the jaccard similarity between 2 TreeSets
		public static double jaccard(TreeSet<String> set_1, TreeSet<String> set_2) {

			if (set_1.size() >= set_2.size()) {
 
				TreeSet<String> s_aux = set_2;
				s_aux.retainAll(set_1);
				
				int inter = s_aux.size();
				set_2.addAll(set_1);
				
				int union = set_2.size();
				
				return (double) inter / union;
			}
			else {
				TreeSet<String> s_aux = set_1;
				s_aux.retainAll(set_2);
				
				int inter = s_aux.size();
				set_1.addAll(set_2);
				
				int union = set_1.size();
				
				return (double) inter / union;

			}

		}


      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
        
    	 List <String> list_id = new ArrayList<String>(Arrays.asList(key.toString().split("--")));
         String first_id = list_id.get(0);
         
         // Get the second id's content with the build HashMap id_contents which is containing each pair of (id,content) for the input file
         String second_id = list_id.get(1);
         String second_content = document_id.get(second_id);
         
         // Using treeset for the values
         TreeSet<String> first_values= new TreeSet<String>(); 
         TreeSet<String> values_aux = new TreeSet<String>();
        
         while (values.iterator().hasNext())
        	 values_aux.add(values.iterator().next().toString());
         
         // This is the first document values
         for (String val : values_aux.iterator().next().split(" ")){
        	 first_values.add(val);
         } 
         
         // This is the second document values
         TreeSet<String> second_values= new TreeSet<String>(Arrays.asList(second_content.split(" ")));
          
         // Jaccard similarity 
         double similarity = jaccard(first_values, second_values);

         // We count the comparisons
         context.getCounter(COUNTER.COMP_COUNT).increment(1);
         
         // We consider 0.6 as a similarity threshold
         if (similarity >=0.6){
        	 StringBuilder text_output = new StringBuilder();
        	 text_output.append("[");
         
	         text_output.append(first_id);
	         
	         text_output.append("--");
	         
	         text_output.append(second_id);
	         text_output.append("]");
	
	         // writing the results
	         context.write(new Text(text_output.toString()),new Text(String.valueOf(similarity)));
       }
      }
      
   }
}