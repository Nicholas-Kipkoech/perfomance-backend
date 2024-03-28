import express, { Express, Request, Response } from "express";
import perfomanceRouter from "./routes/perfomance-routes";
import cors from "cors";
const app: Express = express();

const port = 5002;
app.use(cors());

app.get("/", (req: Request, res: Response) => {
  res.send("Server is running");
});

app.use("/bima/perfomance", perfomanceRouter);

app.listen(port, () => console.log(`server is running at port ${port}`));
