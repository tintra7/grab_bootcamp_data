# Data pipeline to Grab bootcamp project

Run docker
```bash
docker compose up -d
```

Create and start env for Window's user
```bash
conda create -n myenv python=3.10.14
conda activate myenv
pip install -r requirements.txt
```

Create a new file and named it ".env"

Read data from MQTT brocker and send it to kafka brocker
```bash
python mqtt_kafka_bridge.py
```

Start backend server
```bash
python main.py
```

Sample api url

Get current sensor temperature and humidity: http://localhost:5000/api/sensor/current/?sensor_id=66497c500f588f3e5549f8a6

Get history temperature and humidity: http://localhost:5000/api/sensor/last/?sensor_id=66497c500f588f3e5549f8a6&record=10
