package io.pravega.connecter;

import io.pravega.client.stream.StreamConfiguration;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;

public class test {

    public static void main(String[] args) throws IOException {
        Task task= new Task();
        while(task.poll() != null){

        }

    }

}
class Task{
    BufferedReader in;
    public Task() throws FileNotFoundException {
        this.in = new BufferedReader(new FileReader("test.txt"));

    }
    public String poll() throws IOException {
        String str;
        if ((str = in.readLine()) != null) {
            System.out.println(str);
            return str;
        }
        return null;
    }
}
