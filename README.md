# crutch/consumer-amqp

AMQP protocol consumer

# Install

```bash
composer require crutch/consumer-amqp
```

```php
<?php

/** @var Crutch\Consumer\ConsumerHandler $handler */

$consumer = new Crutch\AmqpConsumer\AmqpConsumer($serviceName, 'localhost', 5672, 'user', 'password');
$consumer->consume('topic', $handler);
```
