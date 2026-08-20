# Read messages from the topic
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic earthquakes_json.public.earthquakes \
  --from-beginning \
  --max-messages 2