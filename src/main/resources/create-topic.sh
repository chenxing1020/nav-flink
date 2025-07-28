    # Shell into the kafka container
    docker exec -it kafka /bin/bash

    # Create topics inside the container
    kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    kafka-topics --create --topic user_clicks --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

    # Exit the container
    exit
