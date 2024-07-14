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

  type LoginToken = {
    sub?: string;
    aud?: string;
    jti?: string;
    ip?: string;
  };

  childServer.get("/api/login", async (req, res) => {
    const { sub: id, aud, jti: nonce, ip } = req.user as LoginToken;

    if (aud !== "login") {
      res.status(400);
      return { message: "Token is not for log in" };
    }
    if (id === undefined || nonce === undefined || ip === undefined) {
      res.status(400);
      return { message: "Login token is incomplete" };
    }

    const result = await sql`SELECT * FROM sophomores WHERE id = ${id}`;
    const me = result[0];

    if (me === undefined) {
      res.status(500);
      return {
        ref: "SOPHOMORE_INFO_NOT_FOUND",
        message: `Could not find information for ${id}`
      };
    }

    const loginEvent = {
      ip,
      userId: id,
      eventName: "login",
      nonce
    };
    await sql`INSERT INTO events ${sql(loginEvent)} ON CONFLICT ON CONSTRAINT events_nonce_key DO NOTHING`;

    const accessToken = childServer.jwt.sign(
      {
        me
      },
      {
        sub: id,
        aud: "access"
      }
    );

    return { accessToken };
  });
});

server.listen({ port: 8080 }, (err, address) => {
  if (err) {
    console.error(err);
    process.exit(1);
  }
  console.log(`Server listening at ${address}`);
});
