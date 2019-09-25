docker network create lambda-local
docker run -p 8000:8000 --name dynamodb --network lambda-local --name dynamodb  amazon/dynamodb-local &