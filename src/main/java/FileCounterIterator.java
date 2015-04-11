package geekie.mapred.io;

import java.io.*;

/**
 * Created by nlw on 11/04/15.
 */
public class FileCounterIterator {

    public Long position() {
        return _position;
    }

    public Long fileSize() {
        return _fileSize;
    }

    private Long _fileSize = 0L;
    private Long _position = 0L;
    private RandomAccessFile fp;
    private BufferedReader itr;

    public FileCounterIterator(String filename) throws IOException {
        fp = new RandomAccessFile(filename, "r");
        itr = new BufferedReader(new InputStreamReader(new FileInputStream(fp.getFD())));
        _fileSize = fp.length();
    }

    public Boolean hasNext() throws IOException {
        return this._position < this._fileSize;
    }

    public void seek(Long newPosition) throws IOException {
        this.fp.seek(newPosition);
        this._position = newPosition;
    }

    public String readLine() throws IOException {
        String nextLine = "";
        nextLine = itr.readLine();
        this._position += nextLine.getBytes().length + 1;
        return nextLine;
    }
}
