# streaming

trong trường hợp stream kafka từ docker thì phải thay đổi service redpanda trong file docker-compose.yml thành tên của
container redpanda:

```yaml
--pandaproxy-addr
PLAINTEXT://redpanda:29092,OUTSIDE://redpanda:9092

--advertise-pandaproxy-addr
PLAINTEXT://redpanda:28082,OUTSIDE://redpanda:8082
```
