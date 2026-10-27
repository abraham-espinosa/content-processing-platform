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
  const connection = await amqp.connect(
    process.env.RABBITMQ_URL!
  );

  const channel = await connection.createChannel();

  await channel.assertQueue(QUEUE, {
    durable: true,
    arguments: {
      "x-queue-type": "quorum",
    },
  });

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

        const workerId = process.env.HOSTNAME ?? "unknown-worker";

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

        if (claim.rowCount === 0) {
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
            console.warn(`Lease expired or ownership changed: ${jobId}`);
          }

          if (result.rows[0]?.status === "completed") {
            console.log(`Job ${jobId} already completed`);
            channel.ack(message);
            return;
          }

          // Keep unresolved jobs for manual investigation
          // rather than silently acknowledging them.
          console.error(`Job ${jobId} cannot be claimed`);
          channel.nack(message, false, false);
          return;
        }

        console.log(`Processing job ${jobId}`);

        await sleep(2000);

        await pool.query(
          `UPDATE jobs
           SET status = 'completed',
               processed_at = NOW()
           WHERE id = $1`,
          [jobId]
        );

        console.log(`Completed job ${jobId}`);

        channel.ack(message);
      } catch (error) {
        console.error("Worker error:", error);

        // Reject without requeue to avoid an infinite
        // immediate retry loop.
        channel.nack(message, false, false);
      }
    },
    { noAck: false }
  );
}

start().catch((error) => {
  console.error(error);
  process.exit(1);
});