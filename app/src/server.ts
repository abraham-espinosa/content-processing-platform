import Fastify from "fastify";
import amqp from "amqplib";
import { Pool } from "pg";
import { randomUUID } from "node:crypto";

const app = Fastify({ logger: true });

const QUEUE = "content.processing";

const pool = new Pool({
  host: process.env.PGHOST,
  port: Number(process.env.PGPORT ?? 5432),
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
});

app.get("/health", async () => {
  return { status: "ok" };
});

app.get("/version", async () => {
  return {
    version: process.env.APP_VERSION ?? "local",
  };
});

async function start() {
  const rabbitmqUrl = process.env.RABBITMQ_URL;

  if (!rabbitmqUrl) {
    throw new Error("RABBITMQ_URL is required");
  }

  const connection = await amqp.connect(rabbitmqUrl);
  const channel = await connection.createConfirmChannel();

  // Queue topology is managed by setup-rabbitmq.ts.
  await channel.checkQueue(QUEUE);

  app.post("/jobs", async (_request, reply) => {
    const jobId = randomUUID();

    // Step 1: Persist the job.
    await pool.query(
      `INSERT INTO jobs (id, status)
       VALUES ($1, 'pending')`,
      [jobId]
    );

    // Step 2: Publish the job ID.
    try {
      channel.sendToQueue(
        QUEUE,
        Buffer.from(JSON.stringify({ jobId })),
        { persistent: true }
      );

      // Wait for RabbitMQ publisher confirmation.
      await channel.waitForConfirms();
    } catch (error) {
      app.log.error(error, "Failed to publish job");

      return reply.code(503).send({
        error: "Job saved but publishing failed",
        jobId,
      });
    }

    return reply.code(202).send({
      jobId,
      status: "pending",
    });
  });

  app.get<{ Params: { jobId: string } }>(
    "/jobs/:jobId",
    async (request, reply) => {
      const { jobId } = request.params;

      const result = await pool.query(
        `SELECT id, status, attempts,
                worker_id, created_at, processed_at
         FROM jobs
         WHERE id = $1`,
        [jobId]
      );

      if (result.rowCount === 0) {
        return reply.code(404).send({
          error: "Job not found",
        });
      }

      return result.rows[0];
    }
  );

  await app.listen({
    host: "0.0.0.0",
    port: Number(process.env.PORT ?? 3000),
  });
}

start().catch((error) => {
  app.log.error(error);
  process.exit(1);
});
