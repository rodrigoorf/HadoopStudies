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

public class Exercicio2 {
    // Quantidade de transações financeiras por ano
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        //arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saída
        Path output = new Path(files[1]);

        // criação do job e do nome
        Job j = new Job(c, "exercicio2");

        // registrar classes
        j.setJarByClass(Exercicio2.class);
        j.setMapperClass(MapEx2.class);
        j.setReducerClass(ReduceEx2.class);

        // definir tipos de saída
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // definir arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lançar job e aguardar execução
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapEx2 extends Mapper<LongWritable, Text, Text, IntWritable> {
        // função de map
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            // obtendo linha
            String linha = value.toString();
            // quebrando linha
            String[] palavras = linha.split(";");

            // Pegar a quantidade de transação por ano

            // emitindo <chave, valor> no formato <ano, qtde>
            con.write(new Text(palavras[1]), new IntWritable(1));

        }
    }

    public static class ReduceEx2 extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            // variável para armazenar a soma
            int sum = 0;
            for (IntWritable v : values){
                sum += v.get();
            }

            // emitir <chave, valor> no formato <Ano, soma qtde ocorrências>
            con.write(word, new IntWritable(sum));
        }
    }
} 