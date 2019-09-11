import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Exercicio4Writable implements Writable {


    private float peso;
    private String mercadoria;

    public Exercicio4Writable(int i) {}

    public Exercicio4Writable(float peso, String mercadoria){
        this.peso = peso;
        this.mercadoria = mercadoria;
    }

    public void readFields() throws IOException {
        readFields();
    }

    public void readFields(DataInput in) throws IOException {
        peso = Float.parseFloat(in.readUTF());
        mercadoria = String.parseString(in.readUTF());
    }
    public float getPeso() {
        return peso;
    }
    public void setPeso(float peso) {
        this.peso = peso;
    }
    public String getMercadoria(String mercadoria) {
        this.mercadoria = mercadoria;
        return mercadoria;
    }
    public void setMercadoria(String mercadoria){
        this.mercadoria = mercadoria;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(peso));
        out.writeUTF(String.valueOf(mercadoria));


    }
}
