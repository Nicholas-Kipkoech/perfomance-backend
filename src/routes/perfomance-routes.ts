import { Router } from "express";
import perfomanceController from "../controllers/perfomance-controller";

const perfomanceRouter = Router();

perfomanceRouter.get("/underwriting", (req, res) => {
  perfomanceController.getUnderwritingData(req, res);
});
perfomanceRouter.get("/claims", (req, res) => {
  perfomanceController.getClaimsData(req, res);
});
perfomanceRouter.get("/registered-claims", (req, res) => {
  perfomanceController.getRegisteredClaims(req, res);
});
perfomanceRouter.get("/outstanding-claims", (req, res) => {
  perfomanceController.getOutStandingClaims(req, res);
});
perfomanceRouter.get("/unrenewed-policies", (req, res) => {
  perfomanceController.getUnrenewedPolicies(req, res);
});
perfomanceRouter.get("/undebited-policies", (req, res) => {
  perfomanceController.getUndebitedPolicies(req, res);
});
perfomanceRouter.get("/production", (req, res) => {
  perfomanceController.getProductionPerUnit(req, res);
});
perfomanceRouter.get("/branches", (req, res) => {
  perfomanceController.getOrgBranches(req, res);
});
perfomanceRouter.get("/clients", (req, res) => {
  perfomanceController.getEntityClients(req, res);
});

export default perfomanceRouter;
