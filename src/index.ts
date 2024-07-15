import fastify from "fastify";
import postgres from "postgres";
import "dotenv/config";
import fastifyJwt from "@fastify/jwt";
import fastifyCookie from "@fastify/cookie";

const server = fastify();

const sql = postgres(process.env.DB_URL!, {
  ssl: "require",
  transform: postgres.camel
});

server.register(async (childServer) => {
  childServer.register(fastifyCookie);
  childServer.register(fastifyJwt, {
    secret: process.env.JWT_SECRET!,
    cookie: {
      cookieName: "token",
      signed: false
    }
  });

  childServer.addHook("onRequest", async (req, res) => {
    try {
      await req.jwtVerify({ allowedAud: "access", requiredClaims: ["sub", "aud"] });
    } catch (err) {
      res.send(err);
    }
  });

  type AccessToken = {
    me?: unknown;
    sub: string;
    aud: string;
  };

  childServer.head("/api/nongs", async (req, res) => {
    const { sub: id } = req.user as AccessToken;

    const results = await sql`SELECT * FROM code_lines WHERE phi_id = ${id}`;

    if (results.length > 0) {
      res.status(200);
    } else {
      res.status(404);
    }
  });

  childServer.get("/api/nongs", async (req, res) => {
    const { sub: id } = req.user as AccessToken;

    console.log(req.headers);

    const results = await sql`
      SELECT freshmen.* FROM code_lines
      JOIN freshmen ON nong_id=freshmen.id
      WHERE phi_id = ${id}
    `;

    if (results.length === 0) {
      res.status(404);
      return { message: "Nong has not been randomized yet" };
    }

    return results;
  });

  /**
   * Randomize and returns the successful iteration number
   */
  async function randomizeNong(phiId: string): Promise<number | null> {
    for (let i = 0; i < 100; i++) {
      const result = await sql`
        SELECT freshmen.id FROM freshmen
        LEFT JOIN code_lines ON nong_id = freshmen.id
        WHERE phi_id IS NULL
      `;
      const availableNongs = result.map((e) => e.id);
      const randomNongId = availableNongs[Math.floor(Math.random() * availableNongs.length)];

      let success = false;
      try {
        await sql`INSERT INTO code_lines ${sql({ phiId, nongId: randomNongId })}`;
        success = true;
      } catch (error) {
        const code = (error as { code?: string }).code;
        if (code !== "23505") throw error;
      }

      if (success) {
        return i;
      }
    }

    return null;
  }

  childServer.post("/api/nongs", async (req, res) => {
    const startTime = Date.now();
    const ipToken = req.body as string | undefined;

    if (!ipToken) {
      res.status(400);
      return { message: "Request body is incomplete" };
    }

    const { ip, aud } = childServer.jwt.verify(ipToken, {
      requiredClaims: ["ip", "aud", "sub"]
    }) as {
      ip: string;
      aud: string;
      sub: string;
    };

    if (aud !== "randomize") {
      res.status(400);
      return { message: "Token is not for randomization" };
    }

    const { sub: id } = req.user as AccessToken;

    const exisingRows = await sql`SELECT phi_id FROM code_lines WHERE phi_id = ${id}`;

    if (exisingRows.length > 0) {
      res.status(409);
      return { message: "Already randomized" };
    }

    await sql`INSERT INTO events ${sql({ ip, userId: id, event: "randomize_queued" })}`;

    const iter = await randomizeNong(id);

    if (iter === null) {
      res.status(500);
      return { message: "Randomization failed: reached iteration limit" };
    }

    await sql`INSERT INTO events ${sql({ ip, userId: id, event: "randomize_finished" })}`;
    await sql`INSERT INTO randomization_times ${sql({
      ip,
      userId: id,
      millisElapsed: Date.now() - startTime,
      successIteration: iter
    })}`;
    res.status(201);

    // FOR PERFORMANCE TESTING
    // const result = await sql`SELECT id FROM sophomores`;
    // for (const e of result) {
    //   await sql`INSERT INTO freshmen ${sql({
    //     id: e.id,
    //     name: "Name",
    //     fullName: "fullname",
    //     instagram: "ig",
    //     line: "line",
    //     favoriteThings: "nothing"
    //   })}`;
    // }
    //
    // await Promise.all(
    //   result.map(async (e) => {
    //     randomizeNong(e.id, ip);
    //   })
    // );
  });
});

server.register(async (childServer) => {
  childServer.register(fastifyJwt, {
    secret: process.env.JWT_SECRET!
  });

  childServer.addHook("onRequest", async (req, res) => {
    try {
      await req.jwtVerify({ allowedAud: "login", requiredClaims: ["sub", "aud", "jti", "ip"] });
    } catch (err) {
      res.send(err);
    }
  });

  type LoginToken = {
    sub: string;
    aud: string;
    jti: string;
    ip: string;
  };

  childServer.get("/api/login", async (req, res) => {
    const { sub: id, jti: nonce, ip } = req.user as LoginToken;

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
      event: "login",
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
