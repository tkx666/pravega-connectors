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
        List<SourceRecord> records;
        while((records = source.read()) != null){
            for(int i = 0; i < records.size(); i++)
                sendRecord(records.get(i));
        }
        source.close();
        pravegaWriter.close();

    }

    public void sendRecord(SourceRecord record){
        pravegaWriter.run(pravegaProps.get("routingKey"), record.getValue());
    }
}
