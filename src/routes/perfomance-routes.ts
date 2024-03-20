import { Router } from "express";
import perfomanceController from "../controllers/perfomance-controller";

const perfomanceRouter = Router();

perfomanceRouter.get("/underwriting", (req, res) => {
  perfomanceController.getUnderwritingData(req, res);
});
perfomanceRouter.get("/claims", (req, res) => {
  perfomanceController.getClaimsData(req, res);
});

export default perfomanceRouter;
