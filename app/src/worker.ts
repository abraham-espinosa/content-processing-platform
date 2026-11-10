import amqp from "amqplib";
import { Pool } from "pg";

const QUEUE = "content.processing";

const pool = new Pool({
  host: process.env.PGHOST,
  port: Number(process.env.PGPORT ?? 5432),
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
});

const sleep = (ms: number) =>
  new Promise((resolve) => setTimeout(resolve, ms));

async function start() {
  const rabbitmqUrl = process.env.RABBITMQ_URL;

  if (!rabbitmqUrl) {
    throw new Error("RABBITMQ_URL is required");
  }

  const connection = await amqp.connect(rabbitmqUrl);
  const channel = await connection.createChannel();

  // RabbitMQ topology is created by setup-rabbitmq.ts.
  await channel.checkQueue(QUEUE);

  // Process one unacknowledged message at a time.
  await channel.prefetch(1);

  console.log("Worker waiting for jobs...");

  await channel.consume(
    QUEUE,
    async (message) => {
      if (!message) return;

      try {
        const { jobId } = JSON.parse(
          message.content.toString()
        );

        if (typeof jobId !== "string" || !jobId) {
          throw new Error("Invalid message: jobId is required");
        }

        const workerId =
          process.env.HOSTNAME ?? "unknown-worker";

        // STEP 1: Atomically claim the job.
        const claim = await pool.query(
          `UPDATE jobs
           SET status = 'processing',
               attempts = attempts + 1,
               worker_id = $2,
               lease_expires_at = NOW() + INTERVAL '30 seconds'
           WHERE id = $1
             AND (
               status = 'pending'
               OR (
                 status = 'processing'
                 AND lease_expires_at < NOW()
               )
             )
           RETURNING id`,
          [jobId, workerId]
        );

        // STEP 2: Handle jobs that cannot be claimed.
        if (claim.rowCount === 0) {
          const result = await pool.query(
            `SELECT status FROM jobs WHERE id = $1`,
            [jobId]
          );

          const status = result.rows[0]?.status;

          // Duplicate delivery after successful processing.
          if (status === "completed") {
            console.log(
              `Job ${jobId} already completed`
            );

            channel.ack(message);
            return;
          }

          // For now, unresolved messages go to the DLQ.
          console.warn(
            `Job ${jobId} cannot be claimed. Status: ${
              status ?? "not found"
            }`
          );

          channel.nack(message, false, false);
          return;
        }

        // STEP 3: Process the job.
        console.log(`Processing job ${jobId}`);

        await sleep(2000);

        // STEP 4: Complete only if this worker still
        // owns a valid lease.
        const result = await pool.query(
          `UPDATE jobs
           SET status = 'completed',
               processed_at = NOW(),
               lease_expires_at = NULL
           WHERE id = $1
             AND worker_id = $2
             AND status = 'processing'
             AND lease_expires_at > NOW()
           RETURNING id`,
          [jobId, workerId]
        );

        if (result.rowCount === 0) {
          throw new Error(
            `Cannot complete job ${jobId}: lease expired or ownership changed`
          );
        }

        console.log(`Completed job ${jobId}`);

        // STEP 5: Acknowledge only after database success.
        channel.ack(message);
      } catch (error) {
        console.error("Worker error:", error);

        // Reject without immediate requeue.
        // RabbitMQ sends it to the configured DLQ.
        channel.nack(message, false, false);
      }
    },
    { noAck: false }
  );
}

start().catch((error) => {
  console.error("Worker startup failed:", error);
  process.exit(1);
});
