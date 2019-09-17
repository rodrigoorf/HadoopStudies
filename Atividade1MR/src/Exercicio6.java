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

public class Exercicio6 {
    // Média de valor por peso, de acordo com a mercadoria comercializadas no Brasil (como a
    // base de dados está em inglês utilize Brazil, com Z), separadas de acordo com o ano;
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        //arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saída
        Path output = new Path(files[1]);

        // criação do job e do nome
        Job j = new Job(c, "Exercicio6");

        // registrar classes
        j.setJarByClass(Exercicio6.class);
        j.setMapperClass(MapEx6.class);
        j.setReducerClass(ReduceEx6.class);

        // definir tipos de saída
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);


        // definir arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lançar job e aguardar execução
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapEx6 extends Mapper<LongWritable, Text, Text, FloatWritable> {
        // função de map
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            // obtendo linha
            String linha = value.toString();
            // quebrando linha
            String[] palavras = linha.split(";");
            // verificando se a transação envolve o Brasil
            if(palavras[0].equals("Brazil")){
                //emitir chave = (mercadoria | unidade de peso | ano), valor = (valor)
                con.write(new Text(palavras[3] + " | " + palavras[7] + " | " + palavras[1]), new FloatWritable(Float.parseFloat(palavras[5])));
            }
        }
    }

    public static class ReduceEx6 extends Reducer<Text, FloatWritable, Text, FloatWritable>{
        public void reduce(Text word, Iterable<FloatWritable> values, Context con) throws IOException, InterruptedException {
            // variável para armazenar valores
            float somaValores = 0;
            // variável para fazer média com valores
            int cont = 0;
            for(FloatWritable o : values){
                somaValores += Float.parseFloat(String.valueOf(o));
                cont++;
            }
            float media = somaValores / cont;
            con.write(word, new FloatWritable(media));
        }
    }
} 