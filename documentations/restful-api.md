# RESTful API for Pravega Connect

By default the server runs on the port 8091. You can use the api to manage the connectors. 
Now the server supports application/json as content type.
## Connectors

- ```GET connectors/{connector}/config```

    > Get the configuration of the specific connector

- ```PUT connectors/{connector}/pause```

    > Set the state of connector to pause
                                         
- ```PUT connectors/{connector}/resume```

    > Set the state of connector to resume

- ```PUT connectors/{connector}/stop```

    > Set the state of connector to stop     
- ```DELETE connectors/{connector}```

    > Delete the connector  
- ```POST connectors/{connector}/restart```

    > Restart the connector  
- ```PUT connectors/{connector}/config```
 
    > Update the config of the connector   
 Request Json Object: config (map) – Configuration parameters for the connector. All values should be strings.
                                                                                                                                                                                                    