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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Exercicio1 {
    // Número de transações, por mercadoria, envolvendo o Brasil
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        //arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saída
        Path output = new Path(files[1]);

        // criação do job e do nome
        Job j = new Job(c, "exercicio1");

        // registrar classes
        j.setJarByClass(Exercicio1.class);
        j.setMapperClass(MapEx1.class);
        j.setReducerClass(ReduceEx1.class);

        // definir tipos de saída
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // definir arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lançar job e aguardar execução
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapEx1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        // função de map
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            // obtendo linha
            String linha = value.toString();
            // quebrando linha
            String[] palavras = linha.split(";");

            // verificando se a transação envolve o Brasil
            if(palavras[0].equals("Brazil")){
                // emitindo <chave, valor> no formato <mercadoria, qtde>
                con.write(new Text(palavras[3]), new IntWritable(1));
            }
        }
    }

    public static class ReduceEx1 extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            // variável para armazenar a soma
            int sum = 0;
            for (IntWritable v : values){
                sum += v.get();
            }

            // emitir <chave, valor> no formato <mercadoria, soma qtde ocorrências>
            con.write(word, new IntWritable(sum));
        }
    }
}
