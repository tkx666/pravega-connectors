package io.pravega.connecter.utils;

import io.pravega.client.stream.EventRead;

import java.io.*;
import java.nio.charset.Charset;

public class WriteTest {

    public static void main(String[] args) throws IOException {
        FileOutputStream writerStream = new FileOutputStream("test.txt");
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(writerStream, "UTF-8"));
        try {
            for (int i = 0; i < 5000; i++) {
                out.write(Integer.toString(i));
                out.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                out.flush();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }
}
