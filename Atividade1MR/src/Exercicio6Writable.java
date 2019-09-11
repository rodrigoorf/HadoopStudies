import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Exercicio6Writable implements Writable {


    private float Valor;
    private float Peso;

    public Exercicio6Writable(int i) {}

    public Exercicio6Writable(float Valor, float Peso){
        this.Valor = Valor;
        this.Peso = Peso;
    }

    public void readFields() throws IOException {
        readFields();
    }

    public void readFields(DataInput in) throws IOException {
        Valor = Float.parseFloat(in.readUTF());
        Peso = Float.parseFloat(in.readUTF());
    }
    public float getValor() {
        return Valor;
    }
    public void setValor(float Valor) { this.Valor = Valor; }
    public float getPeso() { return Peso; }
    public void setPeso(float Peso){this.Peso = Peso;}

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(Valor));
        out.writeUTF(String.valueOf(Peso));
    }
}
