package io.pravega.connecter.file.source;

import java.util.List;
import java.util.Map;

public class WorkerFileSource implements Runnable{
    private FileSource fileSource;
    private PravegaWriter pravegaWriter;
    private Map<String, String> pravegaProps;
    public WorkerFileSource(PravegaWriter pravegaWriter, FileSource fileSource, Map<String, String> pravegaProps){
        this.fileSource = fileSource;
        this.pravegaWriter = pravegaWriter;
        this.pravegaProps = pravegaProps;
    }
    @Override
    public void run() {
        List<String> line;
//        while((line = fileSource.readNext()) != null){
//            String str = line.get(0);
//            sendRecord(str);
//        }
        for(int i = 0; i < 30000; i++){
            sendRecord(String.valueOf(i), String.valueOf(i % 5));
        }
        fileSource.close();

    }

    public void sendRecord(String str, String routingKey){
        pravegaWriter.run(routingKey, str);
    }
}
