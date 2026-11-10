import amqp from "amqplib";

const RABBITMQ_URL = process.env.RABBITMQ_URL;


async function setupRabbitMQ() {
  
  if (!RABBITMQ_URL) {
    throw new Error("RABBITMQ_URL is required");
  }
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();

  try {
    // 1. Declare the dead-letter exchange
    await channel.assertExchange("content.dlx", "direct", {
      durable: true,
    });

    // 2. Declare the dead-letter queue
    await channel.assertQueue("content.processing.dlq", {
      durable: true,
      arguments: {
        "x-queue-type": "quorum",
      },
    });

    // 3. Bind the DLQ to the exchange
    await channel.bindQueue(
      "content.processing.dlq",
      "content.dlx",
      "content.processing.dlq"
    );

    // 4. Declare the main processing queue
    await channel.assertQueue("content.processing", {
      durable: true,
      arguments: {
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": "content.dlx",
        "x-dead-letter-routing-key": "content.processing.dlq",
      },
    });

    console.log("RabbitMQ topology initialized successfully");
  } finally {
    await channel.close();
    await connection.close();
  }
}

setupRabbitMQ().catch((error) => {
  console.error("RabbitMQ initialization failed:", error);
  process.exit(1);
});

