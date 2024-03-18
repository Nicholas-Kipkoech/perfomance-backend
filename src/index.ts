import express, { Express, Request, Response } from "express";
import perfomanceRouter from "./routes/perfomance-routes";

const app: Express = express();

const port = 5002;

app.get("/", (req: Request, res: Response) => {
  res.send("Server is running");
});

app.use("/bima/perfomance", perfomanceRouter);

app.listen(port, () =>
  console.log(`server is running at http://localhost:${port}`)
);
