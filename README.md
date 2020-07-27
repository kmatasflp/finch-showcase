# finch-showcase
Used to showcase finch and functional effects in real world scenario

To run locally

1. Use docker compose
```
    docker-compose -f docker-compose-local.yml up -d
```    

2. Create sqs queue using https://github.com/localstack/awscli-local
    ```
    awslocal sqs create-queue --queue-name Test
    ```

3. POST some content using curl i.e
    ```
    curl -v --header "Content-Type: text/plain" \
                --header "Origin: localhost" \
                --header "Referer: example.com" \
        --data 'some-random-data' localhost:8080/beacons
    ```    


