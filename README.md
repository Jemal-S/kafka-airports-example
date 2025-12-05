# kafka-airports-example

### 1. Install/Initialize Kafka with Docker

```
docker-compose up -d
```

### 2. Create Virtual environment

#### Create:
```
python -m venv .venv
```

#### Activate (on powershell):
```
.venv\Scripts\Activate.ps1
```

#### Install requirements from the file:
```
pip install -r requirements.txt
```
### 3. Create the topics

```
docker exec -it broker /opt/kafka/bin/kafka-topics.sh --create --topic airports-partitions --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```

### 4. Run the scripts

Open a new terminal and run:
```
python consumer_print.py
```

Open another terminal and run:
```
python producer.py
```

After running the producer, you should see the consumer receiving the data sent by him.