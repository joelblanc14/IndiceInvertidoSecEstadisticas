package eps.scp;

import java.util.Objects;
import java.io.Serializable;

public class Location implements Comparable<Location>
{
    private static final long serialVersionUID = 1L;

    private int fileId;
    private int line;

    public int getFileId() {
        return fileId;
    }

    public void setFileId(int fileId) {
        this.fileId = fileId;
    }

    public int getLine() {
        return line;
    }

    public void setLine(int line) {
        this.line = line;
    }

    public Location(int fileId, int line) {
        this.fileId = fileId;
        this.line = line;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Location))
            return false;
        Location tuple = (Location) o;
        return fileId == tuple.fileId && line==tuple.line;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileId, line);
    }

    @Override
    public String toString() {
        return "("+fileId+","+line+')';
    }

    @Override
    public int compareTo(Location o) {
        if (this.fileId == o.fileId && this.line == o.line)
            return 0;

        if (this.fileId < o.fileId || (this.fileId == o.fileId && this.line < o.line))
            return -1;
        else
            return 1;
    }
}
