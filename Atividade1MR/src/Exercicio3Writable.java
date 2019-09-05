import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Exercicio3Writable implements Writable {

    private String mercadoria;
    private long quantidade;

    public String getMercadoria() {
        return mercadoria;
    }

    public void setMercadoria(String mercadoria) {
        this.mercadoria = mercadoria;
    }

    public long getQuantidade() {
        return quantidade;
    }

    public void setQuantidade(long quantidade) {
        this.quantidade = quantidade;
    }

    public Exercicio3Writable() {}

    public Exercicio3Writable(String mercadoria, long quantidade){
        this.mercadoria = mercadoria;
        this.quantidade = quantidade;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(String.valueOf(mercadoria));
        dataOutput.writeUTF(String.valueOf(quantidade));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        mercadoria = dataInput.readUTF();
        quantidade = Long.parseLong(dataInput.readUTF());
    }
}
