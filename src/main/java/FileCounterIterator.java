package geekie.mapred.io;

import java.io.*;

/**
 * Created by nlw on 11/04/15.
 * This class reads lines from a buffered file iterator, and also provides a
 * byte count of the data read. This lets you to know the file position you
 * are reading from.
 */
public class FileCounterIterator {

    public Long position() {
        return _position;
    }

    public Long fileSize() {
        return _fileSize;
    }

    public FileCounterIterator newlineLength(Long newNewlineLength) {
        this._newlineLength = newNewlineLength;
        return this;
    }

    private Long _fileSize = 0L;
    private Long _position = 0L;
    private Long _newlineLength = 1L;
    private RandomAccessFile fp;
    private BufferedReader itr;

    public FileCounterIterator(String filename) throws IOException {
        fp = new RandomAccessFile(filename, "r");
        _fileSize = fp.length();
        this.seek(0L);
    }

    public FileCounterIterator seek(Long newPosition) throws IOException {
        this.fp.seek(newPosition);
        this._position = newPosition;
        itr = new BufferedReader(new InputStreamReader(new FileInputStream(fp.getFD())));
        return this;
    }

    public Boolean hasNext() throws IOException {
        return this._position < this._fileSize;
    }

    public String readLine() throws IOException {
        String nextLine = itr.readLine();
        this._position += nextLine.getBytes().length + _newlineLength;
        return nextLine;
    }
}
