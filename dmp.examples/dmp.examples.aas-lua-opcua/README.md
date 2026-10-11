## "examples.aas-lua-opcua" based on the **AAS Polling Consumer**

### Pipeline: 

The **AAS Polling Consumer** consumes data from the **Data Source (AAS)** and sends it to the **Data Sink (OPC UA Server)** via a **Lua** transformer to extract and convert the value from an AAS Property.

### Important Notice:

> As the JSON returned from the AAS property does not specify the OPC UA data type of the node used in the data sink, only variant (string) nodes are currently supported!
