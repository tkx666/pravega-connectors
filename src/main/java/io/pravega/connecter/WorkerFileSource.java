package io.pravega.connecter;

import java.util.List;
import java.util.Map;

public class WorkerFileSource implements Runnable{
    private FileSource fileSource;
    private FileWriter fileWriter;
    private Map<String, String> pravegaProps;
    public WorkerFileSource(FileWriter fileWriter, FileSource fileSource, Map<String, String> pravegaProps){
        this.fileSource = fileSource;
        this.fileWriter = fileWriter;
        this.pravegaProps = pravegaProps;
    }
    @Override
    public void run() {
        List<String> line;
        while((line = fileSource.readNext()) != null){
            String str = line.get(0);
            fileWriter.run(pravegaProps.get("routingKey"), str);

        }

    }
}
