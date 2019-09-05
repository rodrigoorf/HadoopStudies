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

// Mercadoria com maior quantidade (casa 8) de transações financeiras em 2016 no Brasil
public class Exercicio3 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        //arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saída
        Path output = new Path(files[1]);

        // criação do job e do nome
        Job j = new Job(c, "exercicio3");

        // registrar classes
        j.setJarByClass(Exercicio3.class);
        j.setMapperClass(MapEx3.class);
        j.setReducerClass(ReduceEx3.class);

        // definir tipos de saída
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(Exercicio3Writable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(LongWritable.class);

        // definir arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lançar job e aguardar execução
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapEx3 extends Mapper<LongWritable, Text, Text, Exercicio3Writable> {
        // função de map
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            // obtendo linha
            String linha = value.toString();
            // quebrando linha
            String[] palavras = linha.split(";");

            // verificando se a transação envolve o Brasil
            if(palavras[0].equals("Brazil")){
                if(palavras[1].equals("2016")) { // verificando se transação no ano de 2016
                    if(palavras[8].equals("")){ // tratamento para quando a quantidade vem vazia
//                        con.write(new Text("str"), new Exercicio3Writable(palavras[3], 0));
                    } else {
                        // emitindo <chave, valor> no formato <"str", (mercadoria, quantidade)>
                        con.write(new Text("str"), new Exercicio3Writable(palavras[3], Long.parseLong(palavras[8])));
                    }
                }
            }
        }
    }

    public static class ReduceEx3 extends Reducer<Text, Exercicio3Writable, Text, LongWritable> {
        public void reduce(Text word, Iterable<Exercicio3Writable> values, Context con) throws IOException, InterruptedException {
            // variável para armazenar o maior valor
            String maiorMercadoria = "";
            long valorMercadoria = 0;
            for (Exercicio3Writable v : values){
                if(v.getQuantidade() > valorMercadoria){
                    valorMercadoria = v.getQuantidade();
                    maiorMercadoria = v.getMercadoria();
                }
            }

            // emitir <chave, valor> no formato <mercadoria, quantidade>
            con.write(new Text(maiorMercadoria), new LongWritable(valorMercadoria));
        }
    }
}
