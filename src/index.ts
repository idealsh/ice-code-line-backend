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

interface Task<T> {
  id: string;
  perform: () => Promise<T>;
  success: (value: T) => void;
  error: (value: unknown) => void;
}

const serialTasks: Array<Task<any>> = [];
let isExecutingTasks = false;

async function executeTasks() {
  let task: Task<unknown> | undefined;
  while ((task = serialTasks.shift()) !== undefined) {
    await task.perform().then(task.success).catch(task.error);
  }
}

function queueSerialTask<T>(id: string, perform: () => Promise<T>): Promise<T> {
  const promise = new Promise<T>((res, rej) => {
    const task: Task<T> = { id, perform: perform, success: res, error: rej };
    serialTasks.push(task);
  });

  if (!isExecutingTasks && serialTasks.length > 0) {
    isExecutingTasks = true;
    executeTasks().finally(() => {
      isExecutingTasks = false;
    });
  }

  return promise;
}

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
      await req.jwtVerify({ allowedAud: "access", requiredClaims: ["sub", "aud", "me"] });
    } catch (err) {
      res.send(err);
    }
  });

  type AccessToken = {
    me: unknown;
    sub: string;
    aud: string;
  };

  async function checkHasCodeLine(phiId: string): Promise<boolean | undefined> {
    const results = (await sql`
      SELECT code_line1 IS NOT NULL OR code_line2 IS NOT NULL AS has_code_line
      FROM sophomores WHERE id = ${phiId}
    `) as Array<{ hasCodeLine: boolean }>;
    return results[0]?.hasCodeLine;
  }

  childServer.head("/api/nongs", async (req, res) => {
    const { sub: id } = req.user as AccessToken;

    if ((await checkHasCodeLine(id)) ?? false) {
      res.status(200);
    } else {
      res.status(404);
    }
  });

  childServer.get("/api/nongs", async (req, res) => {
    const { sub: id } = req.user as AccessToken;

    const results = await sql`
      SELECT freshmen.* FROM sophomores JOIN freshmen
      ON code_line1 = freshmen.id OR code_line2 = freshmen.id
      WHERE sophomores.id = ${id} ORDER BY freshmen.id
    `;

    if (results.length === 0) {
      res.status(404);
      return { message: "Nong has not been randomized yet" };
    }

    results.forEach((row) => {
      delete row.id;
    });

    return results;
  });

  async function randomizeNongCount(): Promise<number> {
    const ERROR_RANDOMIZING_COUNT = {
      message: "Error occured while randomizing code line count"
    };

    // groups the number of sophomores by preference and by the number of nongs already randomized
    const queryResults = (await sql`
      SELECT double_code_line, code_line_count, count(*)::int
      FROM (
        SELECT
          double_code_line,
          (CASE WHEN code_line1 IS NOT NULL THEN 1 ELSE 0 END +
          CASE WHEN code_line2 IS NOT NULL THEN 1 ELSE 0 END) AS code_line_count
        FROM sophomores
      ) AS _ GROUP BY double_code_line, code_line_count
    `) as Array<{
      doubleCodeLine: DoubleCodeLinePreference;
      codeLineCount: number;
      count: number;
    }>;
    const totalSophomores = queryResults.reduce<number>((acc, cur) => {
      return acc + cur.count;
    }, 0);

    const randomizationStats = queryResults.reduce(
      (acc, cur) => {
        const object = (acc[cur.doubleCodeLine] ??= {});
        object[cur.codeLineCount] = cur.count;
        return acc;
      },
      {} as Partial<Record<DoubleCodeLinePreference, Partial<Record<number, number>>>>
    );

    const okayStats = randomizationStats["okay"];
    if (okayStats === undefined) {
      throw ERROR_RANDOMIZING_COUNT;
    }

    const TOTAL_NONGS = 93; // TODO: update the numbers if necessary
    const slotsForDouble = TOTAL_NONGS - totalSophomores;

    const takenSlots = okayStats[2] ?? 0;

    const remainingSlots = slotsForDouble - takenSlots;
    const remainingSlotCandidates = okayStats[0] ?? 0;

    if (remainingSlots > remainingSlotCandidates) {
      await sql`INSERT INTO events ${sql({ ip: "::1", event: "slots_greater_than_candidates" })}`;
    }

    if (remainingSlotCandidates > 0) {
      console.log(remainingSlots, remainingSlotCandidates);
      return Math.random() < remainingSlots / remainingSlotCandidates ? 2 : 1;
    } else {
      return 1;
    }
  }

  function randomizeNongFromPool(pool: readonly string[], count: number): Array<string> {
    const mutablePool = Array.from(pool);
    const result: Array<string> = [];

    for (let i = 0; i < count; i++) {
      const index = Math.floor(Math.random() * mutablePool.length);

      result.push(...mutablePool.splice(index, 1));
    }

    return result;
  }

  type DoubleCodeLinePreference = "okay" | "neutral" | "no";

  /**
   * Randomize and returns the successful iteration number
   */
  async function randomizeNong(phiId: string): Promise<number | null> {
    const result = await sql`SELECT double_code_line FROM sophomores WHERE id = ${phiId}`;
    const doubleCodeLine = result[0]?.doubleCodeLine as DoubleCodeLinePreference | undefined;

    if (doubleCodeLine === undefined) {
      throw { message: "Could not find double code line preference" };
    }

    for (let i = 0; i < 100; i++) {
      const availableNongs = await sql`
        SELECT freshmen.id AS nong_id FROM sophomores RIGHT JOIN freshmen
        ON code_line1 = freshmen.id OR code_line2 = freshmen.id
        WHERE sophomores.id IS NULL
      `;
      const availableNongIds = availableNongs.map((e) => e.nongId) as Array<string>;

      let nongCount: number;

      switch (doubleCodeLine) {
        case "okay":
          nongCount = await queueSerialTask(phiId, randomizeNongCount);
          break;
        case "neutral":
        case "no":
          nongCount = 1;
          break;
      }

      const hasCodeLine = await checkHasCodeLine(phiId);
      if (hasCodeLine === true) {
        throw { statusCode: 409, message: "Already randomized" };
      } else if (hasCodeLine === undefined) {
        throw { statusCode: 404, message: "User not found" };
      }

      const nongIds = randomizeNongFromPool(availableNongIds, nongCount);

      try {
        /*
         NOTE: there is an issue causing exceptions in triggers (e.g. unique_code_line trigger)
         to be ignored and not handled when using extended queries, so raw sql is used here.
         */

        // the ids aren't expected to contain special characters, but i'm escaping just to be safe
        const codeLine1 = (nongIds[0] ?? "NULL").replace(/['"\\%_]/g, "\\$&");
        const codeLine2 = (nongIds[1] ?? "NULL").replace(/['"\\%_]/g, "\\$&");

        const statement = `
          UPDATE sophomores SET code_line1 = ${codeLine1}, code_line2 = ${codeLine2}
          WHERE id = ${phiId};
        `;
        await sql.unsafe(statement);

        return i;
      } catch (error) {
        const code = (error as { code?: string }).code;
        // throws the error forward if it's not a 23505 unique constraint violation
        if (code !== "23505") throw error;
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

    const { ip } = childServer.jwt.verify(ipToken, {
      allowedAud: "randomize",
      requiredClaims: ["ip", "aud", "sub"]
    }) as {
      ip: string;
      aud: string;
      sub: string;
    };

    const { sub: id } = req.user as AccessToken;

    if (await checkHasCodeLine(id)) {
      res.status(409);
      return { message: "Already randomized" };
    }

    await sql`INSERT INTO events ${sql({ ip, userId: id, event: "randomize_started" })}`;

    try {
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
      return;
    } catch (error) {
      const { statusCode, message } = error as { statusCode?: number; message?: string };

      if (statusCode === undefined && message === undefined) {
        throw error;
      } else {
        res.status(statusCode ?? 500);
        return { message: message ?? "An error occurred while randomizing" };
      }
    }

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

    const result = await sql`SELECT id, name, full_name FROM sophomores WHERE id = ${id}`;
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

server.listen({ port: parseInt(process.env.PORT!) }, (err, address) => {
  if (err) {
    console.error(err);
    process.exit(1);
  }
  console.log(`Server listening at ${address}`);
});
