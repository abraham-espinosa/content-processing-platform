import Fastify from "fastify";

const app = Fastify({
  logger: true,
});

app.get("/health", async () => {
  return {
    status: "ok",
  };
});

app.get("/version", async () => {
  return {
    version: process.env.APP_VERSION ?? "local",
  };
});

const start = async () => {
  try {
    await app.listen({
      host: "0.0.0.0",
      port: Number(process.env.PORT ?? 3000),
    });
  } catch (error) {
    app.log.error(error);
    process.exit(1);
  }
};

start();