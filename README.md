# finch-showcase
Used to showcase finch and functional effects in real world scenario

To test locally
1. build
```
sbt docker
```
2. Use docker compose
```
docker-compose -f docker-compose-local.yml up -d
```    

3. Create sqs queue using https://github.com/localstack/awscli-local
```
awslocal sqs create-queue --queue-name Test
```

4. POST some content using curl i.e
```
curl -v --header "Content-Type: text/plain" \
     --header "Origin: localhost" \
     --header "Referer: example.com" \
     --data 'some-random-data' localhost:8080/beacons
```    


