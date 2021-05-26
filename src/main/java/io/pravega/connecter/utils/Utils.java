package io.pravega.connecter.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Utils {
    public static Properties loadProps(String path){
        Properties props = new Properties();
        if(path != null){
            try {
                InputStream stream = Files.newInputStream(Paths.get(path));
                props.load(stream);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return props;

    }

    public static Map<String, String> propsToMap(Properties props){
        Map<String, String> map = new HashMap<>();
        for(String key: props.stringPropertyNames()){
            map.put(key, props.getProperty(key));
        }
        return map;
    }
}
