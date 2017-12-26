package cn.edu.fudan.hadoop.hello_word_count;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.regex.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


import org.w3c.dom.Document;
//import org.apache.commons.math3.util.OpenIntToDoubleHashMap.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.streaming.StreamXmlRecordReader;
//import org.apache.hadoop.mapreduce.lib.input.XmlInputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.edu.fudan.hadoop.hello_word_count.XmlInputFormatNew;


public class hello_word_count {

	static int textID = 0;
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private final static IntWritable one = new IntWritable(1);
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String info = value.toString();
			//context.write(new Text(info) , new Text(info));
			
			//String info = value.toString();

            InputStream is = new ByteArrayInputStream(value.toString().getBytes("utf-8"));
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder;
            Document doc;
			try {
				dBuilder = dbFactory.newDocumentBuilder();
				doc = dBuilder.parse(is);
	            
				doc.getDocumentElement().normalize();
	            
	            NodeList nList = doc.getElementsByTagName("doc");
	            //context.write(new Text(String.valueOf(nList.getLength())), new Text(String.valueOf(nList.getLength())));
	           
	            
	            for (int tmp = 0; tmp < nList.getLength(); tmp++)
	            {
	            	Node nNode = nList.item(tmp);
	            
	            	
	            	if (nNode.getNodeType() == Node.ELEMENT_NODE)
	            	{
	            		    Element e = (Element) nNode;
	            			//Node revision = e.getElementsByTagName("docno").item(0);
	            			//Node locl = e.getElementsByTagName("url").item(0);
	                    String title = e.getElementsByTagName("contenttitle").item(0).getTextContent();
	                    //context.write(new Text(title), new Text(title));
	                    
	                    //Element eRevision = (Element) revision;
	                    String text = e.getElementsByTagName("content").item(0).getTextContent();
	                    //context.write(new Text(text), new Text(text));
	                    
	                    Configuration conf = context.getConfiguration();
	                    int textId = conf.getInt("TEXT_ID", 0);
	                    
	                    //String nowtextID = String.valueOf(textID);
	                    //context.write(new Text("+TITLE::"), new Text(title + "," + String.valueOf(textID++)));
	                    
	                    String nowtextID = title;
	                    StringTokenizer itr = new StringTokenizer(title + " " + text);
	                    Map<String, Map<String, Integer>> countmap = new HashMap<String , Map <String , Integer>>() ;
	                    Map<String, String> map = new HashMap<String, String>();
	                    Map<String, Integer> wordcount = new HashMap<String, Integer>();
	                    int cnt = 0;
	                    String regEx = "≈≦﹏‰≧├ㄍ㎡﹐ｌ–〖﹕°ˉⅱ→∮▂■〗丄﹖±ˊⅲ↓≮□♀︶﹗ˋ≯┆▄┥◢ㄖ﹙↖┦◣▆﹚※ш↗∕◆◤★㎎﹛Ш↘⌒┊█☆〝㎏﹜↙∵⊕〞゜﹝÷∶﹞Ы∷☉｛＜√○ー﹠＝｜⊙｝＼﹡＞﹢｀～︰﹑′─〓﹒―″〔ˇ‖←│﹔～Ⅱ●﹤＿‥℃℡◎＾﹣＊＃＠＋－＇≠＆¤℉∣△㎜﹋￣┘㎝﹪§Λ∥≤┙『¨‐∈≥㊣﹎ＩГ℅Ⅲ∽﹥＄˙∠〉￠·「」[-\"`~％!@#$%^&*()+=|{}':;',\\[\\].／-+　<>/?~！＂＂@#￥%……&*（）——+|{}【】［］‘；：”“’。，、．？》《．]Σ№↑";
	                    while (itr.hasMoreTokens())
	                    {
	                    	String wordkey = itr.nextToken();
	                    	for (int i = 0 ; i < wordkey.length(); ++i) {
	                    		String key_word = wordkey.charAt(i) + "";
	                    		if(regEx.indexOf(key_word) != -1)	continue ;
	                    		//one
	                    		String one_word = key_word;
	                    		//context.write(new Text(one_word), new Text(one_word + " " + String.valueOf(regEx.indexOf(one_word))));
	                    		if(!countmap.containsKey(key_word)){
	                    			Map<String, Integer> input_map = new HashMap<String ,Integer> ();
	                    			input_map.put(one_word, 1);
	                    			countmap.put(key_word, input_map) ;
	                    			//context.write(new Text(key_word),new Text(one_word + " " + String.valueOf(1) + "--"));
	                    		} else {
	                    			Map<String, Integer> input_map = new HashMap<String ,Integer>();
	                    			input_map = countmap.get(key_word);
	                    			int nowcount = (Integer) input_map.get(one_word) + 1 ;
	                    			input_map.put(one_word, nowcount);
	                    			countmap.put(key_word,input_map);
	                    			//context.write(new Text(key_word),new Text(one_word + " " + String.valueOf(nowcount) + "--"));
	                    		}
	                    		
	                    		//two
	                    		if(i + 1 >= wordkey.length())	continue ;
	           
	                    		String two_word = wordkey.charAt(i + 1) + "";
	                    		if(regEx.indexOf(two_word) != -1)	continue ;
	                    		
	                    		
	                    		two_word =  one_word + two_word;
	                    		//context.write(new Text(key_word), new Text(two_word));
	                    		
	                    		if(!countmap.containsKey(key_word)){
	                    			Map<String, Integer> input_map = new HashMap<String ,Integer> ();
	                    			input_map.put(two_word, 1);
	                    			countmap.put(key_word, input_map) ;
	                    			//context.write(new Text(key_word),new Text(one_word + " " + String.valueOf(1) + "--"));
	                    		} else {
	                    			Map<String, Integer> input_map = new HashMap<String ,Integer>();
	                    			input_map = countmap.get(key_word);
	                    			if(!input_map.containsKey(two_word)) {
	                    				input_map.put(two_word, 1);
	                    			} else {
	                    				int nowcount = (Integer) input_map.get(two_word) + 1 ;
		                    			input_map.put(two_word, nowcount);
	                    			}
	                    			countmap.put(key_word,input_map);
	                    			//context.write(new Text(key_word),new Text(two_word + " " + String.valueOf((Integer) input_map.get(two_word)) + "--"));
	                    		}
	                    		
	                    		if(i + 2 >= wordkey.length())	continue ;
	                    		String three_word = wordkey.charAt(i + 2) + "" ;
	                    		if(regEx.indexOf(three_word) != -1)	continue ;
	                    		three_word =  two_word + three_word;
	                    		
	                    		if(!countmap.containsKey(key_word)){
	                    			Map<String, Integer> input_map = new HashMap<String ,Integer> ();
	                    			input_map.put(three_word, 1);
	                    			countmap.put(key_word, input_map) ;
	                    			//context.write(new Text(key_word),new Text(one_word + " " + String.valueOf(1) + "--"));
	                    		} else {
	                    			Map<String, Integer> input_map = new HashMap<String ,Integer>();
	                    			input_map = countmap.get(key_word);
	                    			if(!input_map.containsKey(three_word)) {
	                    				input_map.put(three_word, 1);
	                    			} else {
	                    				int nowcount = (Integer) input_map.get(three_word) + 1 ;
		                    			input_map.put(three_word, nowcount);
	                    			}
	                    			countmap.put(key_word,input_map);
	                    			//context.write(new Text(key_word),new Text(two_word + " " + String.valueOf((Integer) input_map.get(two_word)) + "--"));
	                    		}
	                    	}
	                    }
	               		
	                    Set<String> set = countmap.keySet();
	                    for (Iterator<String> iter = set.iterator(); iter.hasNext();) {
	                    		String wordkey = (String)iter.next();
	                    		Map<String, Integer> output_map = new HashMap<String , Integer>();
	                    		output_map = countmap.get(wordkey);
	                    		//context.write(new Text(wordkey), new Text(wordkey));
	                    		
	                    		String count_summary = "";
	                    		Set <String> set1 = output_map.keySet();
	                    		for(Iterator<String> iter1 = set1.iterator(); iter1.hasNext();) {
	                    			String perword = (String) iter1.next();
	                    			//context.write(new Text(wordkey), new Text(perword));
	                    			int nowcount = output_map.get(perword);
	                    			count_summary += perword + "-" + String.valueOf(nowcount) + "/";
	                    	
	                    		}
	                    		context.write(new Text(wordkey), new Text(count_summary));
	                    		
	                    } 
	            	}
	           }
	 			
			
			
			} catch (ParserConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			
			Map<String, Map<String, Integer>> countmap = new HashMap<String , Map <String , Integer>>() ;
			String wordkey = key.toString();
			for(Text val :values) {
				String info = val.toString();
				String [] cls = info.split("/");
				for(int i = 0 ; i < cls.length; ++i) {
					//context.write(key, new Text(cls[i]));
					String [] word_and_count = cls[i].split("-");
					if(!countmap.containsKey(wordkey)) {
						Map<String, Integer> input_map = new HashMap<String ,Integer> ();
						int t = Integer.parseInt(word_and_count[1]);
						input_map.put(word_and_count[0], t);
						countmap.put(wordkey, input_map);
					} else {
						Map<String, Integer> input_map = new HashMap<String ,Integer> ();
						input_map = countmap.get(wordkey);
						if(!input_map.containsKey(word_and_count[0])) {
							int t = Integer.parseInt(word_and_count[1]);
							input_map.put(word_and_count[0], t);
							countmap.put(wordkey, input_map);
						} else {
							int t = Integer.parseInt(word_and_count[1]);
							int s = input_map.get(word_and_count[0]);
							input_map.put(word_and_count[0], t + s) ;
							countmap.put(wordkey, input_map);
						}
					}
				}
			}
			
            	Map<String, Integer> output_map = new HashMap<String , Integer>();
            	output_map = countmap.get(wordkey);
            	String count_summary = "";
            	Set <String> set1 = output_map.keySet();
            	for(Iterator<String> iter1 = set1.iterator(); iter1.hasNext();) {
            		String perword = (String) iter1.next();
            		//context.write(new Text(wordkey), new Text(perword));
            		int nowcount = output_map.get(perword);
            		count_summary += perword + "-" + String.valueOf(nowcount) + "/";
            	
            	}
            	context.write(new Text(wordkey), new Text(count_summary));
            		
       
			
			
			/*
			String sum = "";
			String info = values.toString();
			String [] cls = info.split("-");
			for(int i = 0 ; i < cls.length; ++i) {
				context.write(key, new Text(cls[i]));
			}
			*/
			/*
			for (Text val : values) {
				sum += val.toString() + ";";
				if (sum.length() > 1000000)
				{
					context.write(key, new Text(sum));
					context.progress();
					sum = "";
				}
				//context.progress();
			}
			result.set(sum);
			context.write(key, result);
			*/
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        String begin = "<doc>";  
        String end = "</doc>";
        //conf.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader");
        conf.set("START_TAG_KEY", "<doc>");
        conf.set("END_TAG_KEY", "</doc>"); 
        conf.setInt("TEXT_ID",0);
		//conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
		
        Job job = Job.getInstance(conf, "hello_word_count");
		
        
        job.getConfiguration().setFloat(JobContext.SHUFFLE_MEMORY_LIMIT_PERCENT, 0.0001f);
		job.setJarByClass(hello_word_count.class);
		job.setNumReduceTasks(30);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setInputFormatClass(XmlInputFormatNew.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
