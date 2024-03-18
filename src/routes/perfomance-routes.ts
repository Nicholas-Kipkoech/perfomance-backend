import { Router } from "express";
import perfomanceController from "../controllers/perfomance-controller";

const perfomanceRouter = Router();

perfomanceRouter.get("/premiums", (req, res) => {
  perfomanceController.getPremiums(req, res);
});

export default perfomanceRouter;
