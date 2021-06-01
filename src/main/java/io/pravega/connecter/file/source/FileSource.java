package io.pravega.connecter.file.source;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FileSource implements Source {
    Map<String, String> fileProps;
    Map<String, String> pravegaProps;
    BufferedReader in;


    @Override
    public void open(Map<String, String> fileProps, Map<String, String> pravegaProps) {
        this.fileProps = fileProps;
        this.pravegaProps = pravegaProps;
        try {
            this.in = new BufferedReader(new FileReader(fileProps.get("readPath")));
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
    public List<String> readNext() {
        String str;
        List<String> list = new ArrayList<>();
        try {
            if ((str = in.readLine()) != null) {
                System.out.println(str);
                list.add(str);
                return list;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}