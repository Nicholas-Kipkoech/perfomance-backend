import { Router } from "express";
import perfomanceController from "../controllers/perfomance-controller";

const perfomanceRouter = Router();
perfomanceRouter.post("/login", (req, res) => {
  perfomanceController.loginUser(req, res);
});

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
perfomanceRouter.get("/direct-clients", (req, res) => {
  perfomanceController.getDirectClients(req, res);
});
perfomanceRouter.get("/salvages", (req, res) => {
  perfomanceController.getRISalvages(req, res);
});
perfomanceRouter.get("/recovery", (req, res) => {
  perfomanceController.getRIrecovery(req, res);
});
perfomanceRouter.get("/reinsurance", (req, res) => {
  perfomanceController.getReinsurance(req, res);
});
perfomanceRouter.get("/AR-receipts", (req, res) => {
  perfomanceController.getARreceiptsListing(req, res);
});

perfomanceRouter.get("/cm-loss-ratio", (req, res) => {
  perfomanceController.getCMLossRatio(req, res);
});

perfomanceRouter.get("/ri-cessions", (req, res) => {
  perfomanceController.getRICessionsPrem(req, res);
});
perfomanceRouter.get("/ri-paid-cession-sum", (req, res) => {
  perfomanceController.getRIpaidCessionSum(req, res);
});

perfomanceRouter.get("/ri-cessions-register", (req, res) => {
  perfomanceController.getRIcessionReport(req, res);
});
perfomanceRouter.get("/ri-paid-cession-report", (req, res) => {
  perfomanceController.getRiPaidCessionReconReport(req, res);
});
perfomanceRouter.get("/ri-outstanding-cession-report", (req, res) => {
  perfomanceController.getOutstandingRICessions(req, res);
});
export default perfomanceRouter;
