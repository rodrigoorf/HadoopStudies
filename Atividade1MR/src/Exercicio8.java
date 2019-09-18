import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Exercicio8 {
    // Quantidade de transações comerciais de acordo com o fluxo, de acordo com o ano
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        //arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saída
        Path output = new Path(files[1]);

        // criação do job e do nome
        Job j = new Job(c, "Exercicio5");

        // registrar classes
        j.setJarByClass(Exercicio8.class);
        j.setMapperClass(Exercicio8.MapEx8.class);
        j.setReducerClass(Exercicio8.ReduceEx8.class);

        // definir tipos de saída
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // definir arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lançar job e aguardar execução
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapEx8 extends Mapper<LongWritable, Text, Text, IntWritable> {
        // função de map
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            // obtendo linha
            String linha = value.toString();
            // quebrando linha
            String[] palavras = linha.split(";");
            // emitindo chave = fluxo | ano, valor = 1
            con.write(new Text(palavras[4] + " | " + palavras[1]), new IntWritable(1));
        }
    }

    public static class ReduceEx8 extends Reducer<Text, IntWritable, Text, FloatWritable> {
        public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            // variável para guardar soma das quantidades
            float sum = 0;
            for(IntWritable o : values){
                sum += Integer.parseInt(String.valueOf(o));
            }
            con.write(word, new FloatWritable(sum));
        }
    }
}
