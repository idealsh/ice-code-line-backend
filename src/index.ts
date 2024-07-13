import fastify from "fastify";
import postgres from "postgres";
import "dotenv/config";
import fastifyJwt from "@fastify/jwt";

const server = fastify();

const sql = postgres(process.env.DB_URL!, {
  ssl: "require",
  transform: postgres.camel
});

server.register(async (childServer) => {
  childServer.register(fastifyJwt, {
    secret: process.env.JWT_SECRET!
  });

  childServer.addHook("onRequest", async (req, res) => {
    try {
      await req.jwtVerify();
    } catch (err) {
      res.send(err);
    }
  });

  childServer.get("/api/me", async (req, res) => {
    const { id } = req.user as { id: string };

    const result = await sql`SELECT * FROM seniors WHERE id = ${id}`;
    const me = result[0];

    if (me === undefined) {
      throw new Error(`Could not find info for ${id}`);
    } else {
      return me;
    }
  });
});

server.listen({ port: 8080 }, (err, address) => {
  if (err) {
    console.error(err);
    process.exit(1);
  }
  console.log(`Server listening at ${address}`);
});
