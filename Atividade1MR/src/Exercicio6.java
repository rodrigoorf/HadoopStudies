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
        j.setCombinerClass(CombineEx6.class);
        j.setReducerClass(ReduceEx6.class);

        // definir tipos de saída
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Exercicio6Writable.class);


        // definir arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lançar job e aguardar execução
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapEx6 extends Mapper<LongWritable, Text, Text, IntWritable> {
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
            con.write(new Text(palavras[1]), new IntWritable(1));
        }
    }

    public static class ReduceEx6 extends Reducer<Text,Exercicio6Writable, Text, FloatWritable>{
        public void reduce(Text word, Iterable<Exercicio6Writable> values, Context con) throws IOException, InterruptedException {
            // variável para armazenar a soma
            // variável para armazenar a média
            float somaPeso = 0;
            float somaValor = 0;
            // somatorio de Valor e Peso
            for (Exercicio6Writable o : values){
                somaPeso += o.getPeso();
                somaValor += o.getValor();
            }
            //calculando a média
            float media = somaPeso / somaValor;
            // (chave = "media", valor = media)
            con.write(new Text("media"), new FloatWritable(media));

        }
    }
    public static class CombineEx6 extends Reducer<Text, Exercicio6Writable, Text, Exercicio6Writable>{

        public void reduce(Text key, Iterable<Exercicio6Writable> values, Context con) throws IOException, InterruptedException {
            float somaPeso = 0f;
            float somaValor = 0f;
            for(Exercicio6Writable o : values){
                somaValor += o.getValor();
                somaPeso += o.getPeso();

            }

            Exercicio6Writable vlr= new Exercicio6Writable(somaValor,somaPeso);

            con.write(key, vlr);
        }
    }

} 