package top.doe.mr.formats;

public interface InputFormat {
    RecordReader createRecordReader(String param);
}
