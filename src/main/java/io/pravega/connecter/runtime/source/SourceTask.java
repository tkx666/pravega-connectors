package io.pravega.connecter.runtime.source;

import io.pravega.connecter.runtime.PravegaWriter;

import java.util.List;
import java.util.Map;

public class SourceTask implements Runnable{
    private Source source;
    private PravegaWriter pravegaWriter;
    private Map<String, String> pravegaProps;
    public SourceTask(PravegaWriter pravegaWriter, Source source, Map<String, String> pravegaProps){
        this.source = source;
        this.pravegaWriter = pravegaWriter;
        this.pravegaProps = pravegaProps;
    }
    @Override
    public void run() {
        List<String> line;
        // PravegaWriter.init(pravegaProps);
        while((line = source.readNext()) != null){
            String str = line.get(0);
            sendRecord(str);
        }
//        for(int i= 0; i < 500; i++){
//            sendRecord(Integer.toString(i));
//
//        }
        source.close();
        pravegaWriter.close();

    }

    public void sendRecord(String str){
        pravegaWriter.run(pravegaProps.get("routingKey"), str);
    }
}
