package io.pravega.connecter;

import java.util.List;
import java.util.Map;

public interface Source {
    void open(Map<String, String> fileProps, Map<String, String> pravegaProps);

    void close();

    List<String> readNext();

    void write();
}
