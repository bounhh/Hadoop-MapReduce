

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

public class RegisterMain {

    public static class RegisterMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text cle = new Text();
        private FloatWritable valeur = null;
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                JSONArray jsonArray = new JSONArray(value.toString());
                for(int i = 0; i < jsonArray.length(); i++){
                    JSONObject object = jsonArray.getJSONObject(i);
                    JSONObject fields = object.getJSONObject("fields");
                    String name = fields.get("prenoms").toString();
                    String signature = getSignature(name);
                    context.write(new Text(signature), new Text(name));
                }

            }catch (JSONException ex){

            }
        }

        public String getSignature(String name) {
            String phTof = replacePHwithF(name);
            String zTos = replaceQKwithCandZwithS(phTof);
            String dchar = deleteDoubleChar(zTos);
            String  sFin = deleetS(dchar);
            name = deleetVoyelAndH(sFin);
            return name;
        }

        public String deleteDoubleChar(String name){
            StringBuilder str = new StringBuilder(name);
            for(int i=0; i<str.length()-1; i++)
                if (str.charAt(i) == str.charAt(i+1))
                    str.deleteCharAt(i);
            return str.toString();
        }

        public String replacePHwithF(String name){
            StringBuilder str = new StringBuilder(name);
            while(str.indexOf("ph")>=0)
                str.replace(str.indexOf("ph"), str.indexOf("ph")+2, "f");

            return str.toString();
        }

        public String replaceQKwithCandZwithS(String name){
            StringBuilder str = new StringBuilder(name);
            for( int i=0; i<str.length()-1;i++){
                if (str.indexOf("q")>=0){
                    str.replace(str.indexOf("q"), str.indexOf("q")+1, "c");
                }else if (str.indexOf("k")>=0)
                    str.replace(str.indexOf("k"), str.indexOf("k")+1, "c");
                else if (str.indexOf("z")>=0)
                    str.replace(str.indexOf("z"), str.indexOf("z")+1, "s");
            }
            return str.toString();
        }

        public String deleetVoyelAndH(String name){
            StringBuilder str = new StringBuilder(name);
            StringBuilder strNew = new StringBuilder(name);
            int j = 0;
            for( int i=0; i<str.length();i++){
                if (str.charAt(i) == 'a' | str.charAt(i) == 'e' | str.charAt(i) == 'h' | str.charAt(i) == 'i' | str.charAt(i) == 'o' | str.charAt(i) == 'u' | str.charAt(i) == 'y' ){
                    strNew.deleteCharAt(i-j);
                    j =j+1;
                }
            }

            if (strNew.charAt(strNew.length()-1)=='s')
                strNew.deleteCharAt(strNew.length()-1);


            return strNew.toString();
        }

        public String deleetS(String name){
            StringBuilder str = new StringBuilder(name);
            if (str.charAt(str.length()-1)=='s')
                str.deleteCharAt(str.length()-1);

            return str.toString();
        }


    }

    public static class RegisterReducer extends Reducer<Text,Text,NullWritable,Text>{

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

            try{
                JSONObject jsonObject = new JSONObject();
                JSONArray jsonArray = new JSONArray();
                for(Text val : values){
                    JSONObject nameObject = new JSONObject().put("name", val.toString());
                    jsonArray.put(nameObject);
                }
                jsonObject.put("signature", key.toString());
                jsonObject.put("names", jsonArray);
                context.write(NullWritable.get(), new Text(jsonObject.toString()));
            }catch(JSONException e){
                e.printStackTrace();
            }
        }
    }

    public static void main (String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "RegisterMain");
        job.setJarByClass(RegisterMain.class);
        job.setMapperClass(RegisterMapper.class);
        //job.setCombinerClass(RegisterReducer.class);
        job.setReducerClass(RegisterReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}