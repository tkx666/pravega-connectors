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
        this.pravegaProps = pravegaProps;
        this.pravegaWriter = pravegaWriter;
    }
    @Override
    public void run() {
        List<String> line;
        while((line = source.readNext()) != null){
            String str = line.get(0);
            sendRecord(str);
        }
        source.close();
        pravegaWriter.close();

    }

    public void sendRecord(String str){
        pravegaWriter.run(pravegaProps.get("routingKey"), str);
    }
}
