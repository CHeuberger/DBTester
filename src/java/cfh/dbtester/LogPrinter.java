package cfh.dbtester;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

public class LogPrinter extends PrintStream {

    private final PrintStream out;
    private boolean quiet = false;
    
    public LogPrinter(PrintStream out, String filename) throws FileNotFoundException {
        super(new FileOutputStream(filename, true));
        this.out = out;
    }
    
    void setQuiet(boolean quiet) {
        this.quiet = quiet;
    }

    @Override
    public void close() {
        super.close();
    }
    
    @Override
    public void flush() {
        super.flush();
        out.flush();
    }
    
    @Override
    public void write(byte[] buf, int off, int len) {
        super.write(buf, off, len);
        if (!quiet) {
            out.print(new String(buf, off, len));
        }
    }
    
    @Override
    public void write(int b) {
        super.write(b);
        if (!quiet) {
            out.print((char)b);
        }
    }
}
