import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import org.mortbay.jetty.servlet.Context;

import java.io.IOException;

public class Exercicio4 {
    // Média de peso por mercadoria, separadas de acordo com o ano;
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        //arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saída
        Path output = new Path(files[1]);

        // criação do job e do nome
        Job j = new Job(c, "Exercicio4");

        // registrar classes
        j.setJarByClass(Exercicio4.class);
        j.setMapperClass(MapEx4.class);
        j.setCombinerClass(CombineEx4.class);
        j.setReducerClass(ReduceEx4.class);

        // definir tipos de saída
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Exercicio4Writable.class);


        // definir arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lançar job e aguardar execução
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapEx4 extends Mapper<LongWritable, Text, Text, IntWritable> {
        // função de map
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            // obtendo linha
            String linha = value.toString();
            // quebrando linha
            String[] palavras = linha.split(";");
            // emitindo <chave, valor> no formato <"str", (ano,mercadoria)>
            con.write(new Text("str"), new Exercicio4Writable(palavras[2], Long.parseLong(palavras[3])));
            //emitir (chave = 'média', valor=(peso,mercadoria))
            Exercicio4Writable float = new Exercicio4Writable(Peso,6);
            con.write(new Text("Média"),float);

        }
    }

    public static class ReduceEx4 extends Reducer<Text,Exercicio4Writable, Text, FloatWritable>{
        public void reduce(Text word, Iterable<Exercicio4Writable> values, Context con) throws IOException, InterruptedException {
            // variável para armazenar a soma
            // variável para armazenar a média
            float somaPeso = 0;
            String somaMercadoria = new String();
            // somatorio de Mercadoria e peso
            for (Exercicio4Writable o : values){
                somaPeso += o.getPeso();
                somaMercadoria += o.getMercadoria();
            }
            //calculando a média
            float Média = somaPeso / somaMercadoria;
            // (chave = "media", valor = media)
            con.write(new Text("Média"), new FloatWritable(Média));
        }
    }
    public static class CombineEx4 extends Reducer<Text, Exercicio4Writable, Text, Exercicio4Writable>{

        public void reduce(Text key, Iterable<Exercicio4Writable> values, Context con) throws IOException, InterruptedException {
            float somaPeso = 0f;
            String somaMercadoria = new String();
            for(Exercicio4Writable o : values){
                somaPeso += o.getPeso();
                somaMercadoria += o.getMercadoria();
            }

            Exercicio4Writable vlr= new Exercicio4Writable(somaPeso, somaMercadoria);

            con.write(key, vlr);
        }
    }

} 