<?php

declare(strict_types=1);

namespace Crutch\AmqpConsumer;

use ErrorException;
use Crutch\Consumer\Consumer;
use Crutch\Consumer\ConsumerHandler;
use Exception;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use RuntimeException;
use Throwable;

final class AmqpConsumer implements Consumer
{
    /** @var array{0:string,1:null|int,2:string,3:string} */
    private array $settings;
    private string $service;
    private ?AMQPStreamConnection $connection = null;
    private ?AMQPChannel $channel = null;

    /**
     * @param string $service
     * @param string $host
     * @param int $port
     * @param string $user
     * @param string $password
     */
    public function __construct(
        string $service,
        string $host,
        int $port = 5672,
        string $user = 'guest',
        string $password = 'guest'
    ) {
        $this->service = $service;
        $this->settings = [$host, $port, $user, $password];
    }

    /**
     * @throws ErrorException
     */
    public function consume(string $topic, ConsumerHandler $handler): void
    {
        $channel = $this->getChannel();

        $queue = sprintf('%s.%s', $topic, $this->service);
        $channel->queue_declare($queue, false, true, false, false);

        $channel->exchange_declare($topic, 'fanout', false, true, false);
        $channel->queue_bind($queue, $topic);

        $channel->basic_consume(
            $queue,
            $this->service,
            false,
            false,
            false,
            false,
            function (AMQPMessage $message) use ($handler, $topic) {
                try {
                    $handler->handle($message->getBody(), $topic);
                } catch (Throwable $exception) {
                }
                $message->ack();

                if ($message->getBody() === 'quit') {
                    $messageChannel = $message->getChannel();
                    if ($messageChannel instanceof AMQPChannel) {
                        $messageChannel->basic_cancel((string)$message->getConsumerTag());
                    }
                }
            }
        );

        $channel->consume();
    }

    private function getChannel(): AMQPChannel
    {
        if (is_null($this->channel)) {
            $this->channel = $this->getConnection()->channel();
        }
        return $this->channel;
    }

    private function getConnection(): AMQPStreamConnection
    {
        if (is_null($this->connection)) {
            $this->connection = $this->createConnection();
        }
        if (!$this->connection->isConnected()) {
            try {
                $this->connection->reconnect();
            } catch (Exception $exception) {
                throw new RuntimeException('Can not reconnect', 0, $exception);
            }
        }
        return $this->connection;
    }

    private function createConnection(): AMQPStreamConnection
    {
        [$host, $port, $user, $password] = $this->settings;
        $attempt = 1;
        $connection = null;
        while ($attempt < 10) {
            try {
                $connection = new AMQPStreamConnection($host, $port, $user, $password);
            } catch (Throwable $exception) {
            }
            $attempt++;
            usleep(100);
        }
        if (!is_null($connection)) {
            return $connection;
        }
        throw new RuntimeException('Can not create connection');
    }

    public function __destruct()
    {
        if (!is_null($this->channel)) {
            try {
                $this->channel->close();
            } catch (Throwable $exception) {
            }
        }
        if (!is_null($this->connection)) {
            try {
                $this->connection->close();
            } catch (Throwable $exception) {
            }
        }
    }
}
