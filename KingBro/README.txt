Solution:

Bro I have tried to create two spark application , one is spark streaming application which will read realtime data( Data sort of simulates a Ticket like structure , where there are three fields , tickId, category,desc) from kafka ( simulating realtime data processing) and stage it to a .json file.

From here on the json file is further processed by spark batch job which uses spark ml lib to be specific Logistic Regression which analyzes "desc" field of the json file to find occurrences of certain key words like "critical"(could be extended to multiple key words) and predicts whether a Ticket needsto labelled critical or not (1 for critical , 0 for non-critical).
 
Have also written a gradle test "RunSparkMLTests" which will create a topic called "Tickets" ingests data into kafka topic "Tickets" and reads data from kafka topic "Tickets" and stages it into a json file . And that becomes the source file for batch job which will run LogisticRegression and trains sample data and validates the test data against the trained dataset.

Only prerequisite is Kafka needs to be installed and run on localhost port 9092.

Run the command gradle -b build_king_bro.gradle -i build --debug --rerun-tasks

