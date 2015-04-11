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

    public FileCounterIterator(String filename) {
        try {
            fp = new RandomAccessFile(filename, "r");
            itr = new BufferedReader(new InputStreamReader(new FileInputStream(fp.getFD())));
            _fileSize = fp.length();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Boolean hasNext() {
        return this._position < this._fileSize;
    }

    public void seek(Long newPosition) {
        try {
            this.fp.seek(newPosition);
            this._position = newPosition;
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public String readLine() {
        String nextLine = "";
        try {
            nextLine = itr.readLine();
            this._position += nextLine.getBytes().length + 1;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return nextLine;
    }
}
