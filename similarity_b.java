package mypackage.similarity_b;

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
import java.util.HashSet;
import java.util.TreeSet;
import java.util.List;
import java.util.Set;

import java.util.LinkedHashSet;
import java.util.ArrayList;


public class similarity_b extends Configured implements Tool {

   
   public static enum COUNTER {
      COMP_COUNT
      };
           
   public static void main(String[] args) throws Exception {
     
      System.out.println(Arrays.toString(args));      
      
      int res = ToolRunner.run(new Configuration(), new similarity_b(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      
      // Setting up the part 1 of the job

      Job job1 = new Job(getConf(), "similarity_b_1");
      job1.setJarByClass(SimilarityOptimized.class);
      
      job1.setOutputKeyClass(Text.class);
      job1.setOutputValueClass(Text.class);
      
      //Use Mapper1 and Reducer1
      job1.setMapperClass(Map_1.class);
      job1.setReducerClass(Reduce_1.class);

      job1.setInputFormatClass(KeyValueTextInputFormat.class);
      job1.setOutputFormatClass(TextOutputFormat.class);

      // We define the separator as "," 
      job1.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
      
      // Define the output as csv 
      job1.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
     
      FileInputFormat.addInputPath(job1, new Path(args[0]));
      FileOutputFormat.setOutputPath(job1,new Path(args[1]));
      
      job1.waitForCompletion(true);
      
      // Setting up the part 2 of the job
      
      Job job = new Job(getConf(), "similarity_b_2");

      // Getting the jobs ready
      job.setJarByClass(similarity_b.class);
      
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      // Use Mapper and Reducer 
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
      
      Path outFile = new Path(new Path(args[1]),"number_of_similarity_b.txt");
      
      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(outFile, true)));
      writer.write(String.valueOf(counter));
      writer.close();
      
      return 0;
   }
   
   public static class Map_1 extends Mapper<Text, Text,Text,Text> {

  		private Text word = new Text();

  		@Override
  		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

  			//The first map is to get the right number of words

  		    // words in row 
  			Set<String> row_words = new LinkedHashSet<String>(Arrays.asList(value.toString().split(" ")));

   			// Compute the number of words to keep 
   			int nb_words = row_words.size() - (int) Math.ceil(0.6 * row_words.size() ) + 1;
   			// We take the first number of words, the key is the word, the value is the id of the row

   			int i = 1;
   			// Using break as soon as we reached the number of words
   			for (String v : row_words) {       
   				if (i <= nb_words){
	   			    word.set(v);
	   			    context.write(word,key);
	   			    i++;}
   				
   				else {
   					break;
   				}
   			}
   }
}

  public static class Reduce_1 extends Reducer<Text, Text, Text, Text> {
  	  @Override
	  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	  	
	  // This is where we set up the inverted index
 	  StringBuilder reduce_v= new StringBuilder();
 	  HashSet<String> hash_v = new HashSet<String>();  

      // Add the values in setvalue 
      for (Text val : values){
    	  hash_v.add(val.toString());
      }

      // setting up each id line 
      for (String id : hash_v){
      	if (reduce_v.length() !=0){
      		reduce_v.append(" ");
      	}
      	reduce_v.append(id);}

      // Writing
      context.write(key,new Text(reduce_v.toString()));
	}
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
            	  
            	  Set<String> all_ids = new HashSet<String>(Arrays.asList(value.toString().split(" ")));
	              
          		  for (String first_id : all_ids){

        		  		int _first_id= Integer.valueOf(first_id);
        		  		
        		  		for (String second_id : all_ids){
        		  			
        		  			int _second_id = Integer.valueOf(second_id);
        		  		
	                  // for the ones we want, this is comparing ids to not have duplicates
	                  	                                    
	                    if (_second_id > _first_id ){
	                  
		                    StringBuilder key_aux = new StringBuilder();
		                    key_aux.append("[");
		                    key_aux.append(first_id);
		                    
		                    key_aux.append("--");
		                    
		                    key_aux.append(second_id);
		                    
		                    key_aux.append("]");
		                    
		                    context.write(new Text(key_aux.toString()), new Text(aux_hashmap.get(first_id).toString()));
		                    
		                    }
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