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
        j.setReducerClass(ReduceEx4.class);

        // definir tipos de saída
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        // definir arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lançar job e aguardar execução
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapEx4 extends Mapper<LongWritable, Text, Text, FloatWritable> {
        // função de map
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            // obtendo linha
            String linha = value.toString();
            // quebrando linha
            String[] palavras = linha.split(";");
            // tratamento para evitar a 'head' ou string vazia
            if(!palavras[6].equals("weight_kg") && !palavras[6].equals("")){
                // emitindo <chave, valor> no formato <mercadoria | ano, peso)>
                con.write(new Text(palavras[3] + " | " + palavras[1]), new FloatWritable(Float.parseFloat(palavras[6])));
            }
        }
    }

    public static class ReduceEx4 extends Reducer<Text, FloatWritable, Text, FloatWritable>{
        public void reduce(Text word, Iterable<FloatWritable> values, Context con) throws IOException, InterruptedException {
            // variável para armazenar peso
            float somaPeso = 0;
            // variável para contar ocorrências para média
            int cont = 0;
            for (FloatWritable o : values){
                somaPeso += Float.parseFloat(String.valueOf(o));
                cont++;
            }
            //calculando a média
            float media = somaPeso / cont;
            //emitir (chave = 'mercadoria | ano', valor = média de peso)
            con.write(word, new FloatWritable(media));
        }
    }
}