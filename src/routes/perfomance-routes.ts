import { Router } from "express";
import perfomanceController from "../controllers/perfomance-controller";

const perfomanceRouter = Router();

perfomanceRouter.get("/underwriting", (req, res) => {
  perfomanceController.getUnderwritingData(req, res);
});
perfomanceRouter.get("/claims", (req, res) => {
  perfomanceController.getClaimsData(req, res);
});
perfomanceRouter.get("/production", (req, res) => {
  perfomanceController.getProductionPerUnit(req, res);
});
perfomanceRouter.get("/branches", (req, res) => {
  perfomanceController.getOrgBranches(req, res);
});
perfomanceRouter.get("/years", (req, res) => {
  perfomanceController.getYears(req, res);
});
perfomanceRouter.get("/clients", (req, res) => {
  perfomanceController.getEntityClients(req, res);
});

export default perfomanceRouter;
