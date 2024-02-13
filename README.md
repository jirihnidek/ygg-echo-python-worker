# Example of yggdrasil echo worker

Example of yggdrasil echo worker using dasbus Python package.

Requirements:

* https://github.com/rhinstaller/dasbus

To run yggdrasil echo worker run following code

```
python ./main.py
```

It is possible to trigger job (or more) using:

```
echo '"hello"' | go run ./cmd/yggctl generate data-message --directive echo - \
    | pub -broker tcp://localhost:1883 -topic yggdrasil/$(hostname)/data/in
```

It is possible to cancel job using cancel command.

```
echo '{"command":"cancel", "arguments":{"directive":"echo","messageID":"<message_ID>"}}' \
    | go run ./cmd/yggctl generate control-message --type command - \
    | pub -broker tcp://localhost:1883 -topic yggdrasil/$(hostname)/control/in
```
