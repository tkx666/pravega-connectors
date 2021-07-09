package io.pravega.connector.file.source;

import io.pravega.connector.runtime.source.Source;
import io.pravega.connector.runtime.source.SourceRecord;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FileSource implements Source {
    public static String READ_PATH_CONFIG = "readPath";

    Map<String, String> sourceProps;
    Map<String, String> pravegaProps;
    BufferedReader in;


    @Override
    public void open(Map<String, String> sourceProps, Map<String, String> pravegaProps) {
        this.sourceProps = sourceProps;
        this.pravegaProps = pravegaProps;
        try {
            this.in = new BufferedReader(new FileReader(sourceProps.get(READ_PATH_CONFIG)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<SourceRecord> read() {
        String str;
        List<SourceRecord> list = new ArrayList<>();
        try {
            if ((str = in.readLine()) != null) {
                System.out.println(str);
                list.add(new SourceRecord(str));
                return list;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

}
