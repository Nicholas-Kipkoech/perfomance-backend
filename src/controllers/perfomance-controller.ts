import { Request, Response } from "express";
import pool from "../database/database";

class PerformanceController {
  constructor() {}

  async getUnderwritingData(req: Request, res: Response) {
    let connection;
    let results;
    try {
      const fromDate: string | any = req.query.fromDate;
      const toDate: string | any = req.query.toDate;
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
   WHERE     TRUNC (pr_gl_date) BETWEEN :p_fm_dt AND :p_to_dt
         AND a.pr_int_aent_code = b.ENT_AENT_CODE
         AND a.pr_int_ent_code = b.ENT_CODE
         AND pr_os_code = NVL ( :branchCode, pr_os_code)
--and pr_end_code not in ('20003', '20004')
GROUP BY pr_mc_code, pr_int_aent_code, pr_int_end_code
ORDER BY pr_class_name
      `;

      // Execute the query with parameters
      results = (await connection).execute(query, {
        p_fm_dt: fromDate,
        p_to_dt: toDate,
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
      const fromDate: string | any = req.query.fromDate;
      const toDate: string | any = req.query.toDate;
      const branchCode: string | any = req.query.branchCode;
      connection = (await pool).getConnection();
      console.log("connected to database");

      // Construct SQL query with conditional parameter inclusion
      let query = `
       SELECT COUNT (cm_index) total_number, SUM (lc_amount) amount_paid
    FROM all_entity a, cm_payments_vw b
   WHERE     a.ENT_AENT_CODE = b.cm_int_aent_code
         AND a.ENT_CODE = b.CM_INT_ENT_CODE
         AND ent_os_code = NVL ( :branchCode, ent_os_code)
         AND TRUNC (hd_gl_date) BETWEEN :p_fm_dt AND :p_to_dt
GROUP BY ent_os_code
      `;

      // Execute the query with parameters
      results = (await connection).execute(query, {
        p_fm_dt: fromDate,
        p_to_dt: toDate,
        branchCode: branchCode,
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
  async getRegisteredClaims(req: Request, res: Response) {
    let connection;
    let results;
    try {
      const fromDate: string | any = req.query.fromDate;
      const toDate: string | any = req.query.toDate;
      const branchCode: string | any = req.query.branchCode;
      connection = (await pool).getConnection();
      console.log("connected to database");

      // Construct SQL query with conditional parameter inclusion
      let query = `
       SELECT COUNT (*), cm_os_code
    FROM (SELECT DISTINCT
                 a.cm_org_code,
                 a.cm_index,
                 a.cm_pl_index,
                 a.cm_end_index,
                 a.cm_no,
                 a.cm_pl_no,
                 a.cm_end_no,
                 f.pr_lc_si,
                 (SELECT os_name
                    FROM hi_org_structure
                   WHERE os_code = cm_os_code)
                     branch_name,
                 (SELECT os_name
                    FROM hi_org_structure
                   WHERE os_code =
                         (SELECT NVL (os_ref_os_code, os_code)
                            FROM hi_org_structure
                           WHERE os_code = cm_os_code))
                     office_name,
                 a.cm_master_no,
                 a.cm_end_fm_dt
                     cm_pl_master_fm_dt,
                 a.cm_end_to_dt
                     cm_pl_master_to_dt,
                 a.cm_end_to_dt,
                 TRUNC (g.created_on)
                     created_on,
                 a.cm_loss_date,
                 a.cm_int_date,
                 a.cm_cur_code,
                 a.cm_os_code,
                 b.cr_mc_code,
                 INITCAP (
                     pkg_system_admin.get_class_name (cr_org_code, cr_mc_code))
                     class,
                 INITCAP (
                     pkg_system_admin.get_sc_cover_name (cr_org_code,
                                                         cr_mc_code,
                                                         cr_cc_code))
                     cover,
                 b.cr_sc_code,
                 pkg_system_admin.get_subclass_name (cr_org_code, cr_sc_code)
                     sub_class,
                 pkg_system_admin.get_subclass_name (cr_org_code, cr_sc_code)
                     sub_class_name,
                 a.cm_int_aent_code,
                 a.cm_int_ent_code,
                 INITCAP (
                     pkg_system_admin.get_entity_name (a.cm_int_aent_code,
                                                       a.cm_int_ent_code))
                     agent,
                 a.cm_aent_code,
                 a.cm_ent_code,
                 INITCAP (
                     pkg_system_admin.get_entity_name (a.cm_aent_code,
                                                       a.cm_ent_code))
                     insured,
                 a.cm_status,
                 a.cm_desc,
                 a.cm_loss_cause,
                 pkg_system_admin.get_system_desc ('CM_LOSS_CAUSE',
                                                   a.cm_loss_cause)
                     description_of_loss
            FROM cm_claims          a,
                 cm_claims_history  g,
                 cm_claims_risks    b,
                 uw_premium_register f
           WHERE     a.cm_org_code = b.cr_org_code
                 AND a.cm_index = b.cr_cm_index
                 AND a.cm_org_code = f.pr_org_code(+)
                 AND a.cm_pl_index = f.pr_pl_index(+)
                 AND a.cm_end_index = f.pr_end_index(+)
                 AND a.cm_org_code = g.ch_org_code
                 AND a.cm_index = g.ch_cm_index
                 AND g.ch_status IN ('Opened', 'Open')
                 AND TRUNC (g.created_on) BETWEEN :p_fm_dt AND :p_to_dt
                 AND cm_os_code = NVL ( :branchCode, cm_os_code)
                 AND NVL (f.pr_rec_counter, 0) <= 0
                 AND a.cm_register = 'Y')
GROUP BY cm_os_code
      `;

      // Execute the query with parameters
      results = (await connection).execute(query, {
        p_fm_dt: fromDate,
        p_to_dt: toDate,
        branchCode: branchCode,
      });

      const formattedData = (await results).rows?.map((row: any) => ({
        totalNumber: row[0],
        branchCode: row[1],
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
  async getOutStandingClaims(req: Request, res: Response) {
    let connection;
    let results;
    try {
      const fromDate: string | any = req.query.fromDate;
      const toDate: string | any = req.query.toDate;
      const branchCode: string | any = req.query.branchCode;
      connection = (await pool).getConnection();
      console.log("connected to database");

      // Construct SQL query with conditional parameter inclusion
      let query = `
           SELECT DISTINCT
         f.PR_OS_CODE,
         COUNT (a.cm_no) cnt,
         SUM (NVL (cm_lc_value, 0)) amnt
  FROM cm_claims a,
       cm_claims_risks b,
       uw_premium_register f,
       (  SELECT eh_org_code,
                 eh_cm_index,
                 NVL (SUM (NVL (cm_closing_value, 0)), 0) cm_lc_value
            FROM (  SELECT DISTINCT d.eh_org_code,
                                    d.eh_cm_index,
                                    d.eh_ce_index,
                                    d.eh_status,
                                    NVL (d.eh_new_lc_amount, 0) cm_closing_value
                      FROM cm_estimates_history d
                     WHERE d.created_on =
                              (SELECT DISTINCT MAX (g.created_on)
                                 FROM cm_estimates_history g
                                WHERE TRUNC (g.created_on) <= TRUNC (to_date(:p_asatdate))
                                      AND g.eh_org_code = d.eh_org_code
                                      AND g.eh_cm_index = d.eh_cm_index
                                      AND g.eh_ce_index = d.eh_ce_index)
                           AND TRUNC (d.created_on) <= TRUNC (to_date(:p_asatdate))
                           AND d.eh_status NOT IN ('Closed', 'Fully Paid')
                  ORDER BY d.eh_cm_index, d.eh_ce_index)
        GROUP BY eh_org_code, eh_cm_index) e
        ,
     (SELECT DISTINCT a.ch_org_code, a.ch_cm_index, a.created_on,a.ch_status
          FROM cm_claims_history a
         WHERE a.created_on =
                  (SELECT DISTINCT MAX (b.created_on)
                     FROM cm_claims_history b
                    WHERE     TRUNC (b.created_on) <= TRUNC (to_date(:p_asatdate))
                          AND b.ch_org_code = a.ch_org_code
                          AND b.ch_cm_index = a.ch_cm_index)) g
 WHERE     a.cm_org_code = :p_org_code
       AND a.cm_org_code = b.cr_org_code
       AND a.cm_index = b.cr_cm_index
       AND a.cm_org_code = f.pr_org_code(+)
       AND a.cm_pl_index = f.pr_pl_index(+)
       AND a.cm_end_index = f.pr_end_index(+)
           and b.cr_mc_code = f.pr_mc_code(+)
           and b.cr_sc_code = f.pr_sc_code(+)
       AND a.cm_org_code = g.ch_org_code
       AND a.cm_index = g.ch_cm_index
       AND b.cr_org_code = e.eh_org_code(+)
       AND b.cr_cm_index = e.eh_cm_index(+)
       AND g.ch_status NOT IN ('Closed','Closed - No Claim')
       AND a.cm_register = 'Y'
GROUP BY f.PR_OS_CODE
ORDER BY  f.PR_OS_CODE

      `;

      // Execute the query with parameters
      results = (await connection).execute(query, {
        p_asatdate: toDate,
        branchCode: branchCode,
      });

      const formattedData = (await results).rows?.map((row: any) => ({
        org_code: row[0],
        count: row[1],
        totalAmount: row[2],
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
  async getUnrenewedPolicies(req: Request, res: Response) {
    let connection;
    let results;
    try {
      const fromDate: string | any = req.query.fromDate;
      const toDate: string | any = req.query.toDate;
      const branchCode: string | any = req.query.branchCode;
      connection = (await pool).getConnection();
      console.log("connected to database");

      // Construct SQL query with conditional parameter inclusion
      let query = `
            
   SELECT os_name,
         pl_os_code,
         SUM (motor)               motor_count,
         SUM (non_motor)           non_motor_count,
         SUM (pl_fap_motor)        fap_motor,
         SUM (pl_fap_nonmotor)     fap_nonmotor
    FROM (  SELECT DISTINCT
                   pl_org_code,
                   pl_index,
                   pl_no,
                   pl_os_code,
                   CASE
                       WHEN pc_mc_code IN ('080', '070') THEN 1
                       ELSE 0
                   END
                       motor,
                   CASE
                       WHEN pc_mc_code NOT IN ('080', '070') THEN 1
                       ELSE 0
                   END
                       non_motor,
                   pl_end_internal_code,
                   pl_pr_code,
                   pl_os_code || ' Totals'
                       os_code,
                   pkg_sa.org_structure_name (pl_org_code, pl_os_code)
                       os_name,
                   pl_status,
                   pkg_uw.get_product_name (pl_org_code, pl_pr_code)
                       pl_product,
                   TRUNC (pl_to_dt)
                       pl_to_dt,
                   pkg_system_admin.get_entity_name (pl_assr_aent_code,
                                                     pl_assr_ent_code)
                       pl_insured,
                   pkg_system_admin.get_entity_name (pl_int_aent_code,
                                                     pl_int_ent_code)
                       pl_intermediary,
                   pl_int_aent_code,
                   pl_int_ent_code,
                   pl_assr_aent_code,
                   (SELECT pn_note
                      FROM uw_policy_notes
                     WHERE pn_pl_index = a.pl_index AND ROWNUM = 1)
                       reasons,
                   pl_assr_ent_code,
                   NVL (pl_flex19, 'Un-Tagged')
                       pl_reason,
                   NVL (pl_flex20, 'Not Provided')
                       pl_remarks,
                   (NVL (sv_fc_si, 0) * pl_cur_rate)
                       pr_si,
                   (NVL (sv_fc_prem, 0) * pl_cur_rate)
                       pl_tot_prem,
                   CASE
                       WHEN pc_mc_code IN ('080', '070')
                       THEN
                           NVL (PM_REN_FC_PREM, 0)
                       ELSE
                           0
                   END
                       pl_fap_motor,
                   CASE
                       WHEN pc_mc_code NOT IN ('080', '070')
                       THEN
                           NVL (PM_REN_FC_PREM, 0)
                       ELSE
                           0
                   END
                       pl_fap_nonmotor
              FROM (SELECT *
                      FROM (SELECT aa.*,
                                   RANK ()
                                       OVER (PARTITION BY pl_index
                                             ORDER BY pl_end_index DESC)    rnk
                              FROM uh_policy aa
                             WHERE     TRUNC (pl_to_dt + 1) BETWEEN :p_fm_dt
                                                                AND :p_to_dt
                                   AND TRUNC (pl_fm_dt) BETWEEN :p_fm_dt
                                                            AND :p_to_dt)
                     WHERE rnk = 1) a,
                   uh_policy_class b,
                   (  SELECT sv_org_code,
                             sv_pl_index,
                             sv_end_index,
                             NVL (SUM (NVL (sv_fc_si, 0)), 0)       sv_fc_si,
                             NVL (SUM (NVL (sv_fc_prem, 0)), 0)     sv_fc_prem
                        FROM uh_policy_risk_covers
                    GROUP BY sv_org_code, sv_pl_index, sv_end_index) c,
                   (SELECT DISTINCT
                           NVL (os_org_code, '50')           os_org_code,
                           NVL (os_code, '100')              os_code,
                           NVL (os_name, 'Un-Assigned')      os_name,
                           NVL (os_ref_os_code, os_code)     os_ref_os_code,
                           os_type,
                           ent_code,
                           ent_aent_code
                      FROM hi_org_structure, all_entity
                     WHERE ent_os_code = os_code(+)) k,
                   (  SELECT SUM (PM_REN_FC_PREM) PM_REN_FC_PREM, pm_pl_index
                        FROM uw_policy_risk_smi
                    GROUP BY pm_pl_index) r
             WHERE     pl_org_code = b.pc_org_code
                   AND pl_index = b.pc_pl_index
                   AND pl_status IN ('Active', 'Endorsed', 'Open')
                   AND pl_index NOT IN
                           (SELECT pl_index
                              FROM uw_policy
                             WHERE pl_status NOT IN
                                       ('Active', 'Endorsed', 'Open'))
                   AND a.pl_org_code = k.os_org_code
                   AND a.pl_int_aent_code = k.ent_aent_code
                   AND a.pl_int_ent_code = k.ent_code
                   AND a.pl_org_code = c.sv_org_code(+)
                   AND a.pl_org_code = c.sv_org_code(+)
                   AND a.pl_index = c.sv_pl_index(+)
                   AND a.pl_end_index = c.sv_end_index(+)
                   AND a.pl_index = r.pm_pl_index(+)
                   AND (SELECT COUNT (*)
                          FROM uw_endorsements
                         WHERE     pe_org_code = pl_org_code
                               AND pe_pl_index = pl_index
                               AND pe_int_end_code IN ('110', '103')
                               AND pe_status = 'Approved'
                               AND TRUNC (pe_fm_date) BETWEEN :p_fm_dt
                                                          AND :p_to_dt) =
                       0
                   AND (SELECT pl_oneoff
                          FROM uw_policy
                         WHERE pl_index = a.pl_index) = 'N'
                   AND pl_type = 'Normal'
                   AND TRUNC (pl_to_dt + 1) BETWEEN :p_fm_dt AND :p_to_dt
                   AND pl_os_code = NVL ( :branchCode, pl_os_code)
          ORDER BY pl_no)
GROUP BY os_name, pl_os_code

      `;

      // Execute the query with parameters
      results = (await connection).execute(query, {
        p_fm_dt: fromDate,
        p_to_dt: toDate,
        branchCode: branchCode,
      });

      const formattedData = (await results).rows?.map((row: any) => ({
        branchName: row[0],
        branchCode: row[1],
        motorCount: row[2],
        nonMotorCount: row[3],
        motorAmount: row[4],
        nonMotorAmount: row[5],
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
  async getUndebitedPolicies(req: Request, res: Response) {
    let connection;
    let results;
    try {
      const fromDate: string | any = req.query.fromDate;
      const toDate: string | any = req.query.toDate;
      const branchCode: string | any = req.query.branchCode;
      connection = (await pool).getConnection();
      console.log("connected to database");

      // Construct SQL query with conditional parameter inclusion
      let query = `
        SELECT NVL (k.os_ref_os_code, '100')    pl_os_code,
         pc_mc_code,
         SUM (
             NVL (
                 CASE
                     WHEN pl_net_effect IN ('Credit') THEN -1
                     WHEN pl_end_internal_code IN ('100', '105', '106') THEN 0
                     ELSE NVL (sv_fc_prem, 0)
                 END,
                 0))                      pl_prem
    FROM uw_policy      a,
         uw_policy_class d,
         (SELECT DISTINCT
                 NVL (os_code, '100')            os_code,
                 NVL (os_name, 'Un-Assigned')    os_name,
                 NVL (DECODE (os_type, 'Branch', os_code, os_ref_os_code),
                      '100')                     os_ref_os_code,
                 os_type,
                 ent_code,
                 ent_aent_code
            FROM hi_org_structure, all_entity
           WHERE ent_os_code = os_code(+)) k,
         (  SELECT sv_org_code,
                   sv_pl_index,
                   sv_end_index,
                   NVL (SUM (NVL (sv_lc_si, 0)), 0)       sv_lc_si,
                   NVL (SUM (NVL (sv_fc_si, 0)), 0)       sv_fc_si,
                   NVL (SUM (NVL (sv_lc_prem, 0)), 0)     sv_lc_prem,
                   NVL (SUM (NVL (sv_fc_prem, 0)), 0)     sv_fc_prem
              FROM uw_policy_risk_covers
          GROUP BY sv_org_code, sv_pl_index, sv_end_index) c
   WHERE     a.pl_org_code = d.pc_org_code(+)
         AND a.pl_index = d.pc_pl_index(+)
         AND a.pl_end_index = d.pc_end_index(+)
         AND a.pl_org_code = c.sv_org_code(+)
         AND a.pl_index = c.sv_pl_index(+)
         AND a.pl_end_index = c.sv_end_index(+)
         AND a.pl_int_aent_code = k.ent_aent_code
         AND a.pl_int_ent_code = k.ent_code
         AND a.created_by NOT IN ('1000000', 'GRP9ALNI24')
         AND a.pl_status NOT IN ('Active',
                                 'Cancelled',
                                 'Lapsed',
                                 'NotTakeUp',
                                 'Declined',
                                 'Open')
         AND a.pl_end_internal_code IN ('000', '110')
         AND a.pl_bus_type NOT IN ('-1')
         AND DECODE (a.pl_type, 'Renewal', pl_ren_pl_index, pl_index)
                 IS NOT NULL
         AND a.pl_type NOT IN ('Quote', 'RenewalNotice')
         AND NVL (k.os_code, '100') =
             NVL ( :branchCode, NVL (k.os_code, '100'))
--AND NVL (k.os_ref_os_code, '100') = NVL (:p_branch, NVL (k.os_ref_os_code, '100'))

GROUP BY k.os_ref_os_code, pl_os_code, pc_mc_code
UNION
  SELECT NVL (k.os_ref_os_code, '100')    pl_os_code,
         pc_mc_code,
         SUM (
             NVL (
                 CASE
                     WHEN pl_net_effect IN ('Credit') THEN -1
                     WHEN pl_end_internal_code IN ('100', '105', '106') THEN 0
                     ELSE NVL (sv_fc_prem, 0)
                 END,
                 0))                      pl_prem
    FROM uw_policy      a,
         uw_policy_class d,
         uw_endorsements f,
         (SELECT DISTINCT
                 NVL (os_code, '100')            os_code,
                 NVL (os_name, 'Un-Assigned')    os_name,
                 NVL (DECODE (os_type, 'Branch', os_code, os_ref_os_code),
                      '100')                     os_ref_os_code,
                 os_type,
                 ent_code,
                 ent_aent_code
            FROM hi_org_structure, all_entity
           WHERE ent_os_code = os_code(+)) k,
         (  SELECT sv_org_code,
                   sv_pl_index,
                   sv_end_index,
                   NVL (SUM (NVL (sv_lc_si, 0)), 0)       sv_lc_si,
                   NVL (SUM (NVL (sv_fc_si, 0)), 0)       sv_fc_si,
                   NVL (SUM (NVL (sv_lc_prem, 0)), 0)     sv_lc_prem,
                   NVL (SUM (NVL (sv_fc_prem, 0)), 0)     sv_fc_prem
              FROM uw_policy_risk_covers
          GROUP BY sv_org_code, sv_pl_index, sv_end_index) c
   WHERE     a.pl_org_code = d.pc_org_code(+)
         AND a.pl_index = d.pc_pl_index(+)
         AND a.pl_end_index = d.pc_end_index(+)
         AND a.pl_org_code = c.sv_org_code(+)
         AND a.pl_index = c.sv_pl_index(+)
         AND a.pl_end_index = c.sv_end_index(+)
         AND a.pl_org_code = f.pe_org_code
         AND a.pl_index = f.pe_pl_index
         AND a.pl_end_index = f.pe_end_index
         AND a.pl_int_aent_code = k.ent_aent_code
         AND a.pl_int_ent_code = k.ent_code
         AND f.created_by NOT IN ('1000000', 'GRP9ALNI24')
         AND f.pe_status NOT IN ('Approved', 'NotTakenUp', 'Declined')
         AND a.pl_bus_type NOT IN ('-1')
         AND DECODE (a.pl_type, 'Renewal', pl_ren_pl_index, pl_index)
                 IS NOT NULL
         AND a.pl_type NOT IN ('Quote', 'RenewalNotice')
         AND NVL (k.os_code, '100') =
             NVL ( :branchCode, NVL (k.os_code, '100'))
         AND TRUNC (pl_to_dt + 1) BETWEEN :p_fm_dt AND :p_to_dt
GROUP BY k.os_ref_os_code, pl_os_code, pc_mc_code
      `;

      // Execute the query with parameters
      results = (await connection).execute(query, {
        p_fm_dt: fromDate,
        p_to_dt: toDate,
        branchCode: branchCode,
      });

      const formattedData = (await results).rows?.map((row: any) => ({
        branchCode: row[0],
        premiumCode: row[1],
        totalPremium: row[2],
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
  async getEntityClients(req: Request, res: Response) {
    let connection;
    let results;
    try {
      const fromDate: string | any = req.query.fromDate;
      const toDate: string | any = req.query.toDate;
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
         AND EXTRACT (YEAR FROM created_on) BETWEEN :p_fm_dt AND :p_to_dt
         AND ent_os_code = NVL ( :branchCode, ent_os_code)
GROUP BY ent_status, ent_aent_code, ent_os_code
      `;

      // Execute the query with parameters
      results = (await connection).execute(query, {
        p_fm_dt: fromDate,
        p_to_dt: toDate,
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
      const fromDate: string | any = req.query.fromDate;
      const toDate: string | any = req.query.toDate;
      const branchCode: string | any = req.query.branchCode;
      connection = (await pool).getConnection();
      console.log("connected to database");

      // Construct SQL query with conditional parameter inclusion
      let query = `
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
         AND TRUNC (pr_gl_date) BETWEEN :p_fm_dt AND :p_to_dt
         AND b.os_code = NVL ( :branchCode, b.os_code)
GROUP BY a.pr_org_code,
         NVL (b.os_code, '100'),
         NVL (b.os_name, 'Un-Assigned')
ORDER BY pr_org_code, pr_os_code
      `;

      // Execute the query with parameters
      results = (await connection).execute(query, {
        p_fm_dt: fromDate,
        p_to_dt: toDate,
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
