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

public class Exercicio7 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        //arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saída
        Path output = new Path(files[1]);

        // criação do job e do nome
        Job j = new Job(c, "exercicio7");

        // registrar classes
        j.setJarByClass(Exercicio7.class);
        j.setMapperClass(Exercicio7.MapEx7.class);
        j.setReducerClass(Exercicio7.ReduceEx7.class);

        // definir tipos de saída
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Exercicio7Writable.class);

        // definir arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lançar job e aguardar execução
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapEx7 extends Mapper<LongWritable, Text, Text, Exercicio7Writable> {
        // função de map
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            // obtendo linha
            String linha = value.toString();
            // quebrando linha
            String[] palavras = linha.split(";");
            // tratamento para evitar a 'head'
            if(!palavras[5].equals("trade_usd")) {
                // emitindo <chave, valor> no formato <unidadePeso, Ex7Writable>
                con.write(new Text(palavras[7]), new Exercicio7Writable(palavras[3], Float.parseFloat(palavras[5])));
            }
        }
    }

    public static class ReduceEx7 extends Reducer<Text, Exercicio7Writable, Text, Text> {
        public void reduce(Text word, Iterable<Exercicio7Writable> values, Context con) throws IOException, InterruptedException {
            float valorAux = 0;
            String mercadoriaAux = "";
            for(Exercicio7Writable item : values){
                if(item.getPreco() > valorAux){
                    valorAux = item.getPreco();
                    mercadoriaAux = item.getMercadoria();
                }
            }
            // emitir <chave, valor> no formato <unidadePeso, mercadoria | valor>
            con.write(word, new Text("Mercadoria: " + mercadoriaAux + " | " + "Valor: " + valorAux));
        }
    }
}
