import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Exercicio7Writable implements Writable {

    private String mercadoria;
    private float preco;

    public Exercicio7Writable() {}

    public Exercicio7Writable(String mercadoria, float preco){
        this.mercadoria = mercadoria;
        this.preco = preco;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(String.valueOf(mercadoria));
        dataOutput.writeUTF(String.valueOf(preco));
    }

    public String getMercadoria() {
        return mercadoria;
    }

    public void setMercadoria(String mercadoria) {
        this.mercadoria = mercadoria;
    }

    public float getPreco() {
        return preco;
    }

    public void setPreco(float preco) {
        this.preco = preco;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        mercadoria = dataInput.readUTF();
        preco = Float.parseFloat(dataInput.readUTF());
    }
}
