import { Request, Response } from "express";
import pool from "../database/database";

class PerformanceController {
  constructor() {}

  async getUnderwritingData(req: Request, res: Response) {
    let connection;
    let results;
    try {
      const year: number | any = req.query.year;
      const branchCode: string | any = req.query.branchCode;

      connection = (await pool).getConnection();
      console.log("connected to database");

      // Construct SQL query with conditional parameter inclusion
      let query = `
                SELECT pr_mc_code,
         PKG_SYSTEM_ADMIN.GET_CLASS_NAME (50, pr_mc_code)    pr_class_name,
         NVL (
             SUM (
                 CASE
                     WHEN (pr_int_end_code = '000' AND pr_bus_type != '3000')
                     THEN
                         (CASE
                              WHEN a.pr_net_effect IN ('Credit')
                              THEN
                                  (  ((  (  NVL (a.pr_fc_prem, 0)
                                          + NVL (a.pr_fc_eartquake, 0)
                                          + NVL (a.pr_fc_political, 0))
                                       * -1))
                                   * a.pr_cur_rate)
                              ELSE
                                  (  (  NVL (a.pr_fc_prem, 0)
                                      + NVL (a.pr_fc_eartquake, 0)
                                      + NVL (a.pr_fc_political, 0))
                                   * a.pr_cur_rate)
                          END)
                     ELSE
                         0
                 END),
             0)                                              pr_nb,
         NVL (
             SUM (
                 CASE
                     WHEN (pr_int_end_code = '110' AND pr_bus_type != '3000')
                     THEN
                         (CASE
                              WHEN a.pr_net_effect IN ('Credit')
                              THEN
                                  (  ((  (  NVL (a.pr_fc_prem, 0)
                                          + NVL (a.pr_fc_eartquake, 0)
                                          + NVL (a.pr_fc_political, 0))
                                       * -1))
                                   * a.pr_cur_rate)
                              ELSE
                                  (  (  NVL (a.pr_fc_prem, 0)
                                      + NVL (a.pr_fc_eartquake, 0)
                                      + NVL (a.pr_fc_political, 0))
                                   * a.pr_cur_rate)
                          END)
                     ELSE
                         0
                 END),
             0)                                              pr_ren,
         NVL (
             SUM (
                 CASE
                     WHEN (    pr_int_end_code IN ('102',
                                                   '104',
                                                   '108',
                                                   '112')
                           AND pr_bus_type != '3000')
                     THEN
                         (CASE
                              WHEN a.pr_net_effect IN ('Credit')
                              THEN
                                  (  ((  (  NVL (a.pr_fc_prem, 0)
                                          + NVL (a.pr_fc_eartquake, 0)
                                          + NVL (a.pr_fc_political, 0))
                                       * -1))
                                   * a.pr_cur_rate)
                              ELSE
                                  (  (  NVL (a.pr_fc_prem, 0)
                                      + NVL (a.pr_fc_eartquake, 0)
                                      + NVL (a.pr_fc_political, 0))
                                   * a.pr_cur_rate)
                          END)
                     ELSE
                         0
                 END),
             0)                                              pr_refund,
         NVL (
             SUM (
                 CASE
                     WHEN (    pr_int_end_code NOT IN ('102',
                                                       '104',
                                                       '108',
                                                       '112',
                                                       '110',
                                                       '000')
                           AND pr_bus_type != '3000')
                     THEN
                         (CASE
                              WHEN a.pr_net_effect IN ('Credit')
                              THEN
                                  (  ((  (  NVL (a.pr_fc_prem, 0)
                                          + NVL (a.pr_fc_eartquake, 0)
                                          + NVL (a.pr_fc_political, 0))
                                       * -1))
                                   * a.pr_cur_rate)
                              ELSE
                                  (  (  NVL (a.pr_fc_prem, 0)
                                      + NVL (a.pr_fc_eartquake, 0)
                                      + NVL (a.pr_fc_political, 0))
                                   * a.pr_cur_rate)
                          END)
                     ELSE
                         0
                 END),
             0)                                              pr_additional,
         NVL (
             SUM (
                 CASE
                     WHEN (pr_bus_type = '3000')
                     THEN
                         (CASE
                              WHEN a.pr_net_effect IN ('Credit')
                              THEN
                                  (  ((  (  NVL (a.pr_fc_prem, 0)
                                          + NVL (a.pr_fc_eartquake, 0)
                                          + NVL (a.pr_fc_political, 0))
                                       * -1))
                                   * a.pr_cur_rate)
                              ELSE
                                  (  (  NVL (a.pr_fc_prem, 0)
                                      + NVL (a.pr_fc_eartquake, 0)
                                      + NVL (a.pr_fc_political, 0))
                                   * a.pr_cur_rate)
                          END)
                     ELSE
                         0
                 END),
             0)                                              pr_facin,
         NVL (
             SUM (
                 CASE
                     WHEN a.pr_net_effect IN ('Credit')
                     THEN
                         (NVL (a.pr_fc_broker_comm, 0) * -1 * a.pr_cur_rate)
                     ELSE
                         (NVL (a.pr_fc_broker_comm, 0) * a.pr_cur_rate)
                 END),
             0)                                              pr_commission,
         pr_int_aent_code,
         COUNT (pr_mc_code)                                  motor_count,
         pr_mc_code                                          motor_code,
         pr_int_end_code
    FROM uw_premium_register a, all_entity b
   WHERE     EXTRACT (YEAR FROM pr_gl_date) =
             NVL ( :year, EXTRACT (YEAR FROM pr_gl_date))
         AND a.pr_int_aent_code = b.ENT_AENT_CODE
         AND a.pr_int_ent_code = b.ENT_CODE
         AND pr_os_code = NVL ( :branchCode, pr_os_code)
--and pr_end_code not in ('20003', '20004')
GROUP BY pr_mc_code, pr_int_aent_code, pr_int_end_code
ORDER BY pr_class_name
      `;

      // Execute the query with parameters
      results = (await connection).execute(query, {
        year: year,
        branchCode: branchCode,
      });

      const formattedData = (await results).rows?.map((row: any) => ({
        classCode: row[0],
        className: row[1],
        newPolicies: row[2],
        renewals: row[3],
        refund: row[4],
        additional: row[5],
        facin: row[6],
        commision: row[7],
        clientCode: row[8],
        clientsCount: row[9],
        motorCode: row[10],
        renewalCode: row[11],
      }));

      return res.status(200).json({ result: formattedData });
    } catch (error) {
      console.error(error);
      return res.status(500).json({ error: "Internal server error" });
    } finally {
      try {
        if (connection) {
          (await connection).close();
          console.info("Connection closed successfully");
        }
      } catch (error) {
        console.error(error);
      }
    }
  }
  async getClaimsData(req: Request, res: Response) {
    let connection;
    let results;
    try {
      const year: number | any = req.query.year;
      connection = (await pool).getConnection();
      console.log("connected to database");

      // Construct SQL query with conditional parameter inclusion
      let query = `
       SELECT COUNT (cm_index) total_number, SUM (lc_amount) amount_paid
  FROM cm_payments_vw
 WHERE EXTRACT (YEAR FROM hd_gl_date) =
       NVL ( :year, EXTRACT (YEAR FROM hd_gl_date))
      `;

      // Execute the query with parameters
      results = (await connection).execute(query, {
        year: year,
      });

      const formattedData = (await results).rows?.map((row: any) => ({
        totalNumber: row[0],
        amountPaid: row[1],
      }));

      return res.status(200).json({ result: formattedData });
    } catch (error) {
      console.error(error);
      return res.status(500).json({ error: "Internal server error" });
    } finally {
      try {
        if (connection) {
          (await connection).close();
          console.info("Connection closed successfully");
        }
      } catch (error) {
        console.error(error);
      }
    }
  }
  async getOrgBranches(req: Request, res: Response) {
    let connection;
    let results;
    try {
      connection = (await pool).getConnection();
      console.log("connected to database");

      // Construct SQL query with conditional parameter inclusion
      let query = `
        SELECT OS_CODE, OS_NAME FROM hi_org_structure
      `;

      // Execute the query with parameters
      results = (await connection).execute(query);

      const formattedData = (await results).rows?.map((row: any) => ({
        organization_code: row[0],
        organization_name: row[1],
      }));

      return res.status(200).json({ result: formattedData });
    } catch (error) {
      console.error(error);
      return res.status(500).json({ error: "Internal server error" });
    } finally {
      try {
        if (connection) {
          (await connection).close();
          console.info("Connection closed successfully");
        }
      } catch (error) {
        console.error(error);
      }
    }
  }
  async getYears(req: Request, res: Response) {
    let connection;
    let results;
    try {
      connection = (await pool).getConnection();
      console.log("connected to database");

      // Construct SQL query with conditional parameter inclusion
      let query = `
       select distinct extract (year from pr_gl_date) as year from uw_premium_register
      `;

      // Execute the query with parameters
      results = (await connection).execute(query);

      const formattedData = (await results).rows?.map((row: any) => ({
        year: row[0],
      }));

      return res.status(200).json({ result: formattedData });
    } catch (error) {
      console.error(error);
      return res.status(500).json({ error: "Internal server error" });
    } finally {
      try {
        if (connection) {
          (await connection).close();
          console.info("Connection closed successfully");
        }
      } catch (error) {
        console.error(error);
      }
    }
  }
  async getEntityClients(req: Request, res: Response) {
    let connection;
    let results;
    try {
      const year: number | any = req.query.year;
      const branchCode: string | any = req.query.branchCode;
      connection = (await pool).getConnection();
      console.log("connected to database");

      // Construct SQL query with conditional parameter inclusion
      let query = `
      SELECT COUNT (ent_aent_code)     total_clients,
         ent_aent_code             entity_code,
         ent_os_code
    FROM all_entity
   WHERE     ent_status = 'ACTIVE'
         AND EXTRACT (YEAR FROM created_on) =
             NVL ( :year, EXTRACT (YEAR FROM created_on))
         AND ent_os_code = NVL ( :branchCode, ent_os_code)
GROUP BY ent_status, ent_aent_code, ent_os_code
      `;

      // Execute the query with parameters
      results = (await connection).execute(query, {
        year: year,
        branchCode: branchCode,
      });

      const formattedData = (await results).rows?.map((row: any) => ({
        totalClients: row[0],
        clientCode: row[1],
      }));

      return res.status(200).json({ result: formattedData });
    } catch (error) {
      console.error(error);
      return res.status(500).json({ error: "Internal server error" });
    } finally {
      try {
        if (connection) {
          (await connection).close();
          console.info("Connection closed successfully");
        }
      } catch (error) {
        console.error(error);
      }
    }
  }
  async getProductionPerUnit(req: Request, res: Response) {
    let connection;
    let results;
    try {
      const year: number | any = req.query.year;
      const branchCode: string | any = req.query.branchCode;
      connection = (await pool).getConnection();
      console.log("connected to database");

      // Construct SQL query with conditional parameter inclusion
      let query = `
       SELECT a.pr_org_code,
         NVL (b.os_code, '100')          pr_os_code,
         NVL (os_name, 'Un-Assigned')    pr_os_name,
           NVL (
               SUM (
                   CASE
                       WHEN a.pr_net_effect IN ('Credit')
                       THEN
                           (  ((  (  NVL (a.pr_fc_prem, 0)
                                   + NVL (a.pr_fc_eartquake, 0)
                                   + NVL (a.pr_fc_political, 0))
                                * -1))
                            * a.pr_cur_rate)
                       ELSE
                           (  (  NVL (a.pr_fc_prem, 0)
                               + NVL (a.pr_fc_eartquake, 0)
                               + NVL (a.pr_fc_political, 0))
                            * a.pr_cur_rate)
                   END),
               0)
         - NVL (
               SUM (
                   CASE
                       WHEN pr_gl_date >=
                            (SELECT MIN (b.pr_gl_date)
                               FROM uw_premium_register b
                              WHERE     b.pr_org_code = a.pr_org_code
                                    AND b.pr_pl_index = a.pr_pl_index
                                    AND pr_int_end_code = '110')
                       THEN
                           (CASE
                                WHEN a.pr_net_effect IN ('Credit')
                                THEN
                                    (  ((  (  NVL (a.pr_fc_prem, 0)
                                            + NVL (a.pr_fc_eartquake, 0)
                                            + NVL (a.pr_fc_political, 0))
                                         * -1))
                                     * a.pr_cur_rate)
                                ELSE
                                    (  (  NVL (a.pr_fc_prem, 0)
                                        + NVL (a.pr_fc_eartquake, 0)
                                        + NVL (a.pr_fc_political, 0))
                                     * a.pr_cur_rate)
                            END)
                       ELSE
                           0
                   END),
               0)                        pr_nb,
         NVL (
             SUM (
                 CASE
                     WHEN pr_gl_date >=
                          (SELECT MIN (b.pr_gl_date)
                             FROM uw_premium_register b
                            WHERE     b.pr_org_code = a.pr_org_code
                                  AND b.pr_pl_index = a.pr_pl_index
                                  AND pr_int_end_code = '110')
                     THEN
                         (CASE
                              WHEN a.pr_net_effect IN ('Credit')
                              THEN
                                  (  ((  (  NVL (a.pr_fc_prem, 0)
                                          + NVL (a.pr_fc_eartquake, 0)
                                          + NVL (a.pr_fc_political, 0))
                                       * -1))
                                   * a.pr_cur_rate)
                              ELSE
                                  (  (  NVL (a.pr_fc_prem, 0)
                                      + NVL (a.pr_fc_eartquake, 0)
                                      + NVL (a.pr_fc_political, 0))
                                   * a.pr_cur_rate)
                          END)
                     ELSE
                         0
                 END),
             0)                          pr_rn
    FROM uw_premium_register a,
         (SELECT DISTINCT
                 os_org_code,
                 NVL (os_code, '100')            os_code,
                 NVL (os_name, 'Un-Assigned')    os_name,
                 NVL (DECODE (os_type, 'Branch', os_code, os_ref_os_code),
                      '100')                     os_ref_os_code,
                 os_type,
                 ent_code,
                 ent_aent_code
            FROM hi_org_structure, all_entity
           WHERE ent_os_code = os_code(+)) b
   WHERE     a.pr_int_aent_code = b.ent_aent_code
         AND a.pr_int_ent_code = b.ent_code
         AND a.pr_org_code = b.os_org_code
         AND EXTRACT (YEAR FROM pr_gl_date) =
             NVL ( :year, EXTRACT (YEAR FROM pr_gl_date))
         AND b.os_code = NVL ( :branchCode, b.os_code)
GROUP BY a.pr_org_code,
         NVL (b.os_code, '100'),
         NVL (b.os_name, 'Un-Assigned')
ORDER BY pr_org_code, pr_os_code
      `;

      // Execute the query with parameters
      results = (await connection).execute(query, {
        year: year,
        branchCode: branchCode,
      });

      const formattedData = (await results).rows?.map((row: any) => ({
        org_code: row[0],
        branchCode: row[1],
        branchName: row[2],
        newBusiness: row[3],
        renewals: row[4],
      }));

      return res.status(200).json({ result: formattedData });
    } catch (error) {
      console.error(error);
      return res.status(500).json({ error: "Internal server error" });
    } finally {
      try {
        if (connection) {
          (await connection).close();
          console.info("Connection closed successfully");
        }
      } catch (error) {
        console.error(error);
      }
    }
  }
}

const performanceController = new PerformanceController();

export default performanceController;
