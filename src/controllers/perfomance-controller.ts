import { Request, Response } from "express";
import pool from "../database/database";
import oracledb from "oracledb";
import jwt from "jsonwebtoken";

class PerformanceController {
  constructor() {}

  async loginUser(req: Request, res: Response) {
    let connection;
    let result: any;

    try {
      const un: string = req.body.un;
      const pw: string = req.body.pw;
      const devType = req.body.devType;
      const devAddress = req.body.devAddress;

      connection = (await pool).getConnection();
      console.log("connected to database");

      console.log(
        `Calling pkg_sa.auth_user....un...${un} and password is ${pw}`
      );
      // run query to get all ad_applications
      result = (await connection).execute(
        `
          BEGIN
           pkg_sa.auth_user_js (
        :p_un,
        :p_pw,
        :p_dev_type,
        :p_dev_address,
        :v_user_code,
        :v_person_code,
        :v_user_grp,
        :v_user_org,
        :v_os_code,
        :v_trace_menu,
        :v_name_format,
        :v_login_change,
        :v_sys_profile,
        :v_org_desc,
        :v_user_desc,
        :v_result,
        :v_aent_code,
        :v_ent_code,
        :v_ent_name,
        :v_otp_enabled,
        :v_device_exists,
        :v_org_type,
        :v_country_code,
        :v_user_phone,
        :v_user_email,
        :v_main_form,
        :v_view_type,
        :v_bg_color
    );
END;`,
        {
          p_un: un,
          p_pw: pw,
          p_dev_type: devType,
          p_dev_address: devAddress,
          v_user_code: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_person_code: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_user_grp: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_user_org: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_os_code: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_trace_menu: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_name_format: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_login_change: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_sys_profile: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_org_desc: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_user_desc: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_result: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_aent_code: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_ent_code: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_ent_name: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_otp_enabled: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_device_exists: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_org_type: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_country_code: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_user_phone: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_user_email: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_main_form: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_view_type: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
          v_bg_color: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
        }
      );

      let user = {};

      if ((await result).outBinds.v_result === "0") {
        const accessToken = jwt.sign(
          {
            userCode: (await result).outBinds.v_user_code,
            personCode: (await result).outBinds.v_person_code,
            userGrp: (await result).outBinds.v_user_grp,
            userOrg: (await result).outBinds.v_user_org,
            osCode: (await result).outBinds.v_os_code,
            traceMenu: (await result).outBinds.v_trace_menu,
            nameFormat: (await result).outBinds.v_name_format,
            loginChange: (await result).outBinds.v_login_change,
            sysProfile: (await result).outBinds.v_sys_profile,
            orgDesc: (await result).outBinds.v_org_desc,
            aentCode: (await result).outBinds.v_aent_code,
            entCode: (await result).outBinds.v_ent_code,
            entName: (await result).outBinds.v_ent_name,
            otpEnabled: (await result).outBinds.v_otp_enabled,
            deviceExists: (await result).outBinds.v_device_exists,
            orgType: (await result).outBinds.v_org_type,
            countryCode: (await result).outBinds.v_country_code,
            userPhone: (await result).outBinds.v_user_phone,
            userEmail: (await result).outBinds.v_user_email,
            mainForm: (await result).outBinds.v_main_form,
            viewType: (await result).outBinds.v_view_type,
            bgColor: (await result).outBinds.v_bg_color,
          },
          "hhsyyahashhshsggaga",
          { expiresIn: "1d" }
        );

        user = {
          userCode: (await result).outBinds.v_user_code,
          personCode: (await result).outBinds.v_person_code,
          userGrp: (await result).outBinds.v_user_grp,
          userOrg: (await result).outBinds.v_user_org,
          osCode: (await result).outBinds.v_os_code,
          traceMenu: (await result).outBinds.v_trace_menu,
          nameFormat: (await result).outBinds.v_name_format,
          loginChange: (await result).outBinds.v_login_change,
          sysProfile: (await result).outBinds.v_sys_profile,
          orgDesc: (await result).outBinds.v_org_desc,
          aentCode: (await result).outBinds.v_aent_code,
          entCode: (await result).outBinds.v_ent_code,
          entName: (await result).outBinds.v_ent_name,
          otpEnabled: (await result).outBinds.v_otp_enabled,
          deviceExists: (await result).outBinds.v_device_exists,
          orgType: (await result).outBinds.v_org_type,
          countryCode: (await result).outBinds.v_country_code,
          userPhone: (await result).outBinds.v_user_phone,
          userEmail: (await result).outBinds.v_user_email,
          mainForm: (await result).outBinds.v_main_form,
          viewType: (await result).outBinds.v_view_type,
          bgColor: (await result).outBinds.v_bg_color,
        };

        return res.status(200).json({
          success: true,
          message: "User logged in successfully!",
          accessToken: accessToken,
        });
      } else {
        return res.status(400).json({
          error: "Please login with correct credentials",
          success: false,
        });
      }
    } catch (error) {
      console.error(error);
    } finally {
      if (connection) {
        try {
          // Always close connections
          await (await connection).close();
          console.log("close connection success");
        } catch (err) {
          console.error(err);
        }
      }
    }
  }

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
       SELECT ss.*,
       (SELECT NVL (SUM (eh_new_lc_amount), 0)     v_amount
          FROM (  SELECT NVL (a.eh_new_lc_amount, 0)     eh_new_lc_amount
                    FROM cm_estimates_history a
                   WHERE     a.eh_org_code = ss.cm_org_code
                         AND a.eh_cm_index = ss.cm_index
                         AND a.eh_status IN ('New')
                         AND a.created_on =
                             (SELECT MIN (created_on)
                                FROM cm_estimates_history b
                               WHERE     b.eh_org_code = a.eh_org_code
                                     AND b.eh_cm_index = a.eh_cm_index
                                     AND TRUNC (b.created_on) =
                                         TRUNC (ss.created_on)
                                     AND b.eh_new_lc_amount > 0)
                ORDER BY a.created_on DESC))    total_provision
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
               NVL ((SELECT DISTINCT TRUNC (MIN (n.created_on))
                       FROM cm_claims_history n
                      WHERE     n.ch_org_code = g.ch_org_code
                            AND n.ch_cm_index = g.ch_cm_index
                            AND n.created_on >= g.created_on
                            AND n.ch_status IN ('Fully Paid',
                                                'Partialy Paid',
                                                'Closed',
                                                'Closed - No Claim')),
                    :p_to_dt)
                   created_on_nxt,
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
          FROM cm_claims            a,
               cm_claims_history    g,
               cm_claims_risks      b,
               uw_premium_register  f
         WHERE     a.cm_org_code = :p_org_code
               AND a.cm_org_code = b.cr_org_code
               AND a.cm_index = b.cr_cm_index
               AND a.CM_OS_CODE = NVL ( :branchCode, a.cm_os_code)
               AND a.cm_org_code = f.pr_org_code(+)
               AND a.cm_pl_index = f.pr_pl_index(+)
               AND a.cm_end_index = f.pr_end_index(+)
               AND a.cm_org_code = g.ch_org_code
               AND a.cm_index = g.ch_cm_index
               AND g.ch_status IN ('Opened', 'Open')
               AND (g.created_on) BETWEEN ( :p_fm_dt) AND ( :p_to_dt)
               AND NVL (f.pr_rec_counter, 0) <= 0
               AND a.cm_register = 'Y') ss
      `;

      // Execute the query with parameters
      results = (await connection).execute(query, {
        p_org_code: "50",
        p_fm_dt: fromDate,
        p_to_dt: toDate,
        branchCode: branchCode,
      });

      const formattedData = (await results).rows?.map((row: any) => ({
        branchCode: row[19],
        totalProvision: row[36],
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
 WHERE    
        a.cm_org_code = b.cr_org_code
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
                             WHERE     TRUNC (pl_to_dt + 1) BETWEEN TRUNC (
                                                                        TO_DATE (
                                                                            :p_fm_dt))
                                                                AND TRUNC (
                                                                        TO_DATE (
                                                                            :p_to_dt))
                                   AND TRUNC (pl_fm_dt) NOT BETWEEN TRUNC (
                                                                        TO_DATE (
                                                                            :p_fm_dt))
                                                                AND TRUNC (
                                                                        TO_DATE (
                                                                            :p_to_dt)))
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
                           NVL (os_org_code, :p_org_code)     os_org_code,
                           NVL (os_code, '100')               os_code,
                           NVL (os_name, 'Un-Assigned')       os_name,
                           NVL (os_ref_os_code, os_code)      os_ref_os_code,
                           os_type,
                           ent_code,
                           ent_aent_code
                      FROM hi_org_structure, all_entity
                     WHERE ent_os_code = os_code(+)) k,
                   (  SELECT SUM (PM_REN_FC_PREM) PM_REN_FC_PREM, pm_pl_index
                        FROM uw_policy_risk_smi
                    GROUP BY pm_pl_index) r
             WHERE     pl_org_code = :p_org_code
                   AND pl_org_code = b.pc_org_code
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
                               AND TRUNC (pe_fm_date) BETWEEN TRUNC (
                                                                  TO_DATE (
                                                                      :p_fm_dt))
                                                          AND TRUNC (
                                                                  TO_DATE (
                                                                      :p_to_dt))) =
                       0
                   AND (SELECT pl_oneoff
                          FROM uw_policy
                         WHERE pl_index = a.pl_index) = 'N'
                   AND pl_os_code = NVL ( :branchCode, a.PL_OS_CODE)
                   AND pl_type = 'Normal'
                   AND TRUNC (pl_to_dt + 1) BETWEEN TRUNC (TO_DATE ( :p_fm_dt))
                                                AND TRUNC (TO_DATE ( :p_to_dt))
          ORDER BY pl_no)
GROUP BY os_name
      `;

      // Execute the query with parameters
      results = (await connection).execute(query, {
        p_fm_dt: fromDate,
        p_to_dt: toDate,
        p_org_code: "50",
        branchCode: branchCode,
      });

      const formattedData = (await results).rows?.map((row: any) => ({
        branchName: row[0],
        motorCount: row[1],
        nonMotorCount: row[2],
        motorAmount: row[3],
        nonMotorAmount: row[4],
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
       SELECT a.pl_org_code,
       a.pl_index,
       a.pl_end_index,
       a.pl_pr_code,
       DECODE (
           a.pl_type,
           'Renewal', (SELECT DISTINCT pl_no
                         FROM uw_policy d
                        WHERE     d.pl_org_code = a.pl_org_code
                              AND d.pl_index = a.pl_ren_pl_index),
           pl_no)
           pl_no,
       (SELECT LISTAGG (pn_note, CHR (10)) WITHIN GROUP (ORDER BY pn_note)    "reasons"
          FROM uw_policy_notes
         WHERE pn_pl_index = a.pl_index)
           reasons,
       NVL (a.pl_end_no, 'NEW')
           pl_end_no,
       TRUNC (a.pl_fm_dt)
           pl_fm_dt,
       TRUNC (a.pl_to_dt)
           pl_to_dt,
       TRUNC (a.pl_issue_date)
           pl_issue_date,
       TRUNC (a.created_on)
           created_on,
       TRUNC (a.updated_on)
           updated_on,
       a.pl_assr_ent_code,
       pkg_system_admin.get_entity_name (a.pl_assr_aent_code,
                                         a.pl_assr_ent_code)
           pl_client,
       pkg_cust.get_user_name (a.pl_org_code, a.created_by)
           user_created_initials,
       pkg_cust.get_user_name (a.pl_org_code, a.updated_by)
           user_updated_initials,
       a.pl_int_ent_code,
       pkg_system_admin.get_entity_name (a.pl_int_aent_code,
                                         a.pl_int_ent_code)
           pl_intermediary,
       NVL (k.os_ref_os_code, '100')
           pl_os_code,
       UPPER (
           NVL (pkg_sa.org_structure_name (a.pl_org_code, k.os_ref_os_code),
                'Un-Assigned'))
           pl_os_name,
       d.pc_mc_code,
       NVL (pkg_system_admin.get_class_name (a.pl_org_code, d.pc_mc_code),
            'UNCHECKED')
           pl_class,
       d.pc_sc_code,
       pkg_system_admin.get_subclass_name (a.pl_org_code, d.pc_sc_code)
           pl_subclass,
       (DECODE ( :p_currency, NULL, NVL (sv_lc_si, 0), NVL (sv_fc_si, 0)))
           pl_si,
       NVL (
           CASE
               WHEN pl_net_effect IN ('Credit')
               THEN
                   ((  DECODE ( :p_currency,
                               NULL, NVL (sv_lc_prem, 0),
                               NVL (sv_fc_prem, 0))
                     * -1))
               WHEN pl_end_internal_code IN ('100', '105', '106')
               THEN
                   0
               ELSE
                   (DECODE ( :p_currency,
                            NULL, NVL (sv_lc_prem, 0),
                            NVL (sv_fc_prem, 0)))
           END,
           0)
           pl_prem,
       DECODE (a.updated_by, NULL, 'UN-PROCESSED', 'PROCESSING')
           pl_status
  FROM uw_policy        a,
       uw_policy_class  d,
       (SELECT DISTINCT
               NVL (os_org_code, :p_org_code)    os_org_code,
               NVL (os_code, '100')              os_code,
               NVL (os_name, 'Un-Assigned')      os_name,
               NVL (DECODE (os_type, 'Branch', os_code, os_ref_os_code),
                    '100')                       os_ref_os_code,
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
       AND a.pl_org_code = k.os_org_code
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
       AND a.pl_cur_code = NVL ( :p_currency, a.pl_cur_code)
       AND a.pl_org_code = :p_org_code
       AND a.pl_type NOT IN ('Quote', 'RenewalNotice')
       AND NVL (k.os_code, '100') = NVL ( :p_branch, NVL (k.os_code, '100'))
       --AND NVL (k.os_ref_os_code, '100') = NVL (:p_branch, NVL (k.os_ref_os_code, '100'))
       AND DECODE ( :p_filter_by, '1', (a.created_on), (a.pl_fm_dt)) BETWEEN :p_fm_dt
                                                                         AND :p_to_dt
UNION
SELECT a.pl_org_code,
       a.pl_index,
       a.pl_end_index,
       a.pl_pr_code,
       a.pl_no,
       (SELECT LISTAGG (pn_note, CHR (10)) WITHIN GROUP (ORDER BY pn_note)    "reasons"
          FROM uw_policy_notes
         WHERE pn_pl_index = a.pl_index)
           reasons,
       NVL (a.pl_end_no, 'New')
           pl_end_no,
       TRUNC (f.pe_fm_date)
           pl_fm_dt,
       TRUNC (f.pe_to_date)
           pl_to_dt,
       TRUNC (f.created_on)
           pl_issue_date,
       TRUNC (f.created_on)
           created_on,
       TRUNC (f.updated_on)
           updated_on,
       a.pl_assr_ent_code,
       pkg_system_admin.get_entity_name (a.pl_assr_aent_code,
                                         a.pl_assr_ent_code)
           pl_client,
       pkg_cust.get_user_name (a.pl_org_code, f.created_by)
           user_created_initials,
       pkg_cust.get_user_name (a.pl_org_code, f.updated_by)
           user_updated_initials,
       a.pl_int_ent_code,
       pkg_system_admin.get_entity_name (a.pl_int_aent_code,
                                         a.pl_int_ent_code)
           pl_intermediary,
       NVL (k.os_ref_os_code, '100')
           pl_os_code,
       UPPER (
           NVL (pkg_sa.org_structure_name (a.pl_org_code, k.os_ref_os_code),
                'Un-Assigned'))
           pl_os_name,
       d.pc_mc_code,
       NVL (pkg_system_admin.get_class_name (a.pl_org_code, d.pc_mc_code),
            'UNCHECKED')
           pl_class,
       d.pc_sc_code,
       pkg_system_admin.get_subclass_name (a.pl_org_code, d.pc_sc_code)
           pl_subclass,
       (DECODE ( :p_currency, NULL, NVL (sv_lc_si, 0), NVL (sv_fc_si, 0)))
           pl_si,
       NVL (
           CASE
               WHEN pl_net_effect IN ('Credit')
               THEN
                   ((  DECODE ( :p_currency,
                               NULL, NVL (sv_lc_prem, 0),
                               NVL (sv_fc_prem, 0))
                     * -1))
               WHEN pl_end_internal_code IN ('100', '105', '106')
               THEN
                   0
               ELSE
                   (DECODE ( :p_currency,
                            NULL, NVL (sv_lc_prem, 0),
                            NVL (sv_fc_prem, 0)))
           END,
           0)
           pl_prem,
       DECODE (f.updated_by, NULL, 'UN-PROCESSED', 'PROCESSING')
           pl_status
  FROM uw_policy        a,
       uw_policy_class  d,
       uw_endorsements  f,
       (SELECT DISTINCT
               NVL (os_org_code, :p_org_code)    os_org_code,
               NVL (os_code, '100')              os_code,
               NVL (os_name, 'Un-Assigned')      os_name,
               NVL (DECODE (os_type, 'Branch', os_code, os_ref_os_code),
                    '100')                       os_ref_os_code,
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
       AND a.pl_org_code = k.os_org_code
       AND a.pl_int_aent_code = k.ent_aent_code
       AND a.pl_int_ent_code = k.ent_code
       AND f.created_by NOT IN ('1000000', 'GRP9ALNI24')
       AND f.pe_status NOT IN ('Approved', 'NotTakenUp', 'Declined')
       AND a.pl_cur_code = NVL ( :p_currency, a.pl_cur_code)
       AND a.pl_org_code = :p_org_code
       AND a.pl_bus_type NOT IN ('-1')
       AND DECODE (a.pl_type, 'Renewal', pl_ren_pl_index, pl_index)
               IS NOT NULL
       AND a.pl_type NOT IN ('Quote', 'RenewalNotice')
       AND NVL (k.os_code, '100') = NVL ( :p_branch, NVL (k.os_code, '100'))
       --  AND NVL (k.os_ref_os_code, '100') = NVL (:p_branch, NVL (k.os_ref_os_code, '100'))

       AND DECODE ( :p_filter_by, '1', (f.created_on), (f.pe_fm_date)) BETWEEN :p_fm_dt
                                                                           AND :p_to_dt
ORDER BY created_on ASC
      `;

      // Execute the query with parameters
      results = (await connection).execute(query, {
        p_fm_dt: fromDate,
        p_to_dt: toDate,
        p_branch: branchCode,
        p_currency: "",
        p_filter_by: "",
        p_org_code: "50",
      });
      console.log(await results);

      const formattedData = (await results).rows?.map((row: any) => ({
        branchCode: row[18],
        premiumCode: row[20],
        totalPremium: row[25],
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
      const branchCode: string | any = req.query.branchCode;
      connection = (await pool).getConnection();
      console.log("connected to database");

      // Construct SQL query with conditional parameter inclusion
      let query = `
      SELECT *
  FROM (  SELECT COUNT (ent_aent_code)        total_clients,
                 ent_aent_code                entity_code,
                 NVL (ent_os_code, '100')     ent_os_code
            FROM all_entity
           WHERE ent_status = 'ACTIVE'
        GROUP BY ent_aent_code, ent_os_code)
 WHERE ent_os_code = NVL ( :branchCode, ent_os_code) SELECT COUNT (ent_aent_code)     total_clients,
         ent_aent_code             entity_code,
         ent_os_code
    FROM all_entity
   WHERE     ent_status = 'ACTIVE'
         AND ent_os_code = NVL ( :branchCode, ent_os_code)
GROUP BY ent_status, ent_aent_code, ent_os_code
      `;

      // Execute the query with parameters
      results = (await connection).execute(query, {
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
              SELECT NVL (b.os_code, '100')          pr_os_code,
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
        branchName: row[1],
        newBusiness: row[2],
        renewals: row[3],
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
  async getRISalvages(req: Request, res: Response) {
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
       SELECT hd_os_code, NVL (a.lc_amount, 0) hd_rcpt_amt
    FROM cm_recovery_receipts_vw a
   WHERE     (hd_gl_date) BETWEEN ( :p_fm_dt) AND ( :p_to_dt)
         AND hd_os_code = NVL ( :branchCode, hd_os_code)
ORDER BY hd_os_code
      `;

      // Execute the query with parameters
      results = (await connection).execute(query, {
        p_fm_dt: fromDate,
        p_to_dt: toDate,
        branchCode: branchCode,
      });

      const formattedData = (await results).rows?.map((row: any) => ({
        branchCode: row[0],
        receiptAmount: row[1],
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
  async getRIrecovery(req: Request, res: Response) {
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
      SELECT bh_org_code,
         hd_os_code,
         bh_mc_code,
         UPPER (pkg_system_admin.get_class_name (bh_org_code, bh_mc_code))
             class,
         SUM (NVL (j.lc_amount, 0))
             paid_amount,
         SUM (
               NVL (
                   ROUND (
                       (  NVL (j.lc_amount, 0)
                        * DECODE (retention_perc_null,
                                  NULL, r.retention_perc,
                                  h.retention_perc)
                        / 100)),
                   0)
             - pkg_cm.get_cm_paid_ri_amount ( :p_org_code,
                                             cm_index,
                                             hd_no,
                                             NULL,
                                             NULL,
                                             'XOL',
                                             'XOL',
                                             :p_fm_dt,
                                             :p_to_dt,
                                             'LC',
                                             2))
             retention_amnt,
         SUM (CASE
                  WHEN (pr_end_code NOT IN ('20003', '20004'))
                  THEN
                        NVL (pkg_cm.get_cm_paid_ri_amount ( :p_org_code,
                                                           cm_index,
                                                           hd_no,
                                                           cr_ri_batch_no,
                                                           cr_ri_cr_index,
                                                           'CQS',
                                                           'CQS',
                                                           :p_fm_dt,
                                                           :p_to_dt,
                                                           'LC',
                                                           2),
                             0)
                      + NVL (pkg_cm.get_cm_paid_ri_amount ( :p_org_code,
                                                           cm_index,
                                                           hd_no,
                                                           cr_ri_batch_no,
                                                           cr_ri_cr_index,
                                                           'SURPLUS',
                                                           'Surplus 1',
                                                           :p_fm_dt,
                                                           :p_to_dt,
                                                           'LC',
                                                           2),
                             0)
                      + NVL (pkg_cm.get_cm_paid_ri_amount ( :p_org_code,
                                                           cm_index,
                                                           hd_no,
                                                           cr_ri_batch_no,
                                                           cr_ri_cr_index,
                                                           'SURPLUS',
                                                           'Surplus 2',
                                                           :p_fm_dt,
                                                           :p_to_dt,
                                                           'LC',
                                                           2),
                             0)
                      + NVL (pkg_cm.get_cm_paid_ri_amount ( :p_org_code,
                                                           cm_index,
                                                           hd_no,
                                                           cr_ri_batch_no,
                                                           cr_ri_cr_index,
                                                           'QS',
                                                           'QS',
                                                           :p_fm_dt,
                                                           :p_to_dt,
                                                           'LC',
                                                           2),
                             0)
                  ELSE
                      0
              END)
             treaty_amt,
         SUM (CASE
                  WHEN (pr_end_code IN ('20003', '20004'))
                  THEN
                        NVL (pkg_cm.get_cm_paid_ri_amount ( :p_org_code,
                                                           cm_index,
                                                           hd_no,
                                                           cr_ri_batch_no,
                                                           cr_ri_cr_index,
                                                           'CQS',
                                                           'CQS',
                                                           :p_fm_dt,
                                                           :p_to_dt,
                                                           'LC',
                                                           2),
                             0)
                      + NVL (pkg_cm.get_cm_paid_ri_amount ( :p_org_code,
                                                           cm_index,
                                                           hd_no,
                                                           cr_ri_batch_no,
                                                           cr_ri_cr_index,
                                                           'SURPLUS',
                                                           'Surplus 1',
                                                           :p_fm_dt,
                                                           :p_to_dt,
                                                           'LC',
                                                           2),
                             0)
                      + NVL (pkg_cm.get_cm_paid_ri_amount ( :p_org_code,
                                                           cm_index,
                                                           hd_no,
                                                           cr_ri_batch_no,
                                                           cr_ri_cr_index,
                                                           'SURPLUS',
                                                           'Surplus 2',
                                                           :p_fm_dt,
                                                           :p_to_dt,
                                                           'LC',
                                                           2),
                             0)
                      + NVL (pkg_cm.get_cm_paid_ri_amount ( :p_org_code,
                                                           cm_index,
                                                           hd_no,
                                                           cr_ri_batch_no,
                                                           cr_ri_cr_index,
                                                           'QS',
                                                           'QS',
                                                           :p_fm_dt,
                                                           :p_to_dt,
                                                           'LC',
                                                           2),
                             0)
                  ELSE
                      0
              END)
             commesa_amt,
         SUM (NVL (pkg_cm.get_cm_paid_ri_amount ( :p_org_code,
                                                 cm_index,
                                                 hd_no,
                                                 cr_ri_batch_no,
                                                 cr_ri_cr_index,
                                                 'FAC OUT',
                                                 'FAC Out',
                                                 :p_fm_dt,
                                                 :p_to_dt,
                                                 'LC',
                                                 2),
                   0))
             fac_amt,
         SUM (NVL (pkg_cm.get_cm_paid_ri_amount ( :p_org_code,
                                                 cm_index,
                                                 hd_no,
                                                 NULL,
                                                 NULL,
                                                 'XOL',
                                                 'XOL',
                                                 :p_fm_dt,
                                                 :p_to_dt,
                                                 'LC',
                                                 2),
                   0))
             xol_amnt
    FROM cm_payments_vw     j,
         uh_policy          x,
         ri_batch_header    bh,
         vw_premium_register prem,
         (  SELECT DISTINCT
                   rs_org_code,
                   rs_ri_batch_no,
                   rs_ri_cr_index,
                   rs_trt_code,
                   NVL (SUM (DECODE (rs_line_type_int, 'Retention', rs_percent)),
                        0)
                       retention_perc,
                   SUM (DECODE (rs_line_type_int, 'Retention', rs_percent))
                       retention_perc_null,
                   NVL (SUM (DECODE (rs_line_type_int, 'Surplus 1', rs_percent)),
                        0)
                       surp1_perc,
                   NVL (SUM (DECODE (rs_line_type_int, 'Surplus 2', rs_percent)),
                        0)
                       surp2_perc,
                   NVL (SUM (DECODE (rs_line_type_int, 'QS', rs_percent)), 0)
                       qs_perc,
                   NVL (SUM (DECODE (rs_line_type_int, 'CQS', rs_percent)), 0)
                       cqs_perc,
                   NVL (SUM (DECODE (rs_line_type_int, 'FAC Out', rs_percent)),
                        0)
                       facout_perc,
                   0
                       xol_perc,
                   NVL (SUM (DECODE (NULL, rs_lc_si, rs_fc_si)), 0)
                       si,
                   NVL (SUM (DECODE (rs_line_type_int, 'Retention', rs_lc_si)),
                        0)
                       retention_si,
                   NVL (SUM (DECODE (rs_line_type_int, 'Surplus 1', rs_lc_si)),
                        0)
                       surp1_si,
                   NVL (SUM (DECODE (rs_line_type_int, 'Surplus 2', rs_lc_si)),
                        0)
                       surp2_si,
                   NVL (SUM (DECODE (rs_line_type_int, 'QS', rs_lc_si)), 0)
                       qs_si,
                   NVL (SUM (DECODE (rs_line_type_int, 'CQS', rs_lc_si)), 0)
                       cqs_si,
                   NVL (SUM (DECODE (rs_line_type_int, 'FAC Out', rs_lc_si)), 0)
                       facout_si,
                   0
                       xol_si
              FROM uw_policy_ri_shares
             WHERE rs_type = 'Final' AND rs_line_type_int NOT IN ('Balance')
          GROUP BY rs_org_code,
                   rs_ri_batch_no,
                   rs_ri_cr_index,
                   rs_trt_code) h,
         (  SELECT DISTINCT
                   rh.cm_org_code,
                   rh.cm_cm_index,
                   rh.cm_risk_index,
                   NVL (SUM (DECODE (cm_line_type, 'RETENTION', cm_perc)), 0)    retention_perc
              FROM cm_claims_ri_header rh, cm_claims_ri_alloc ra
             WHERE     rh.cm_cm_index = ra.cm_cm_index
                   AND rh.cm_risk_index = ra.cm_risk_index
                   AND rh.cm_org_code = ra.cm_org_code
                   AND cm_line_type = 'RETENTION'
          GROUP BY rh.cm_org_code, rh.cm_cm_index, rh.cm_risk_index) r
   WHERE     hd_org_code = :p_org_code
         AND hd_os_code = NVL ( :branchCode, hd_os_code)
         AND j.hd_org_code = x.pl_org_code
         AND j.cm_pl_index = x.pl_index
         AND j.cm_end_index = x.pl_end_index
         AND x.pl_index = prem.pr_pl_index
         AND x.pl_end_index = prem.pr_end_index
         AND j.cr_ri_batch_no = bh.bh_batch_no(+)
         AND j.hd_org_code = h.rs_org_code(+)
         AND j.cr_ri_batch_no = h.rs_ri_batch_no(+)
         AND j.cr_ri_cr_index = h.rs_ri_cr_index(+)
         AND j.hd_org_code = r.cm_org_code(+)
         AND j.cm_index = r.cm_cm_index(+)
         AND j.cr_risk_index = r.cm_risk_index(+)
         AND (hd_gl_date) BETWEEN ( :p_fm_dt) AND ( :p_to_dt)
GROUP BY bh_org_code, bh_mc_code, hd_os_code
      `;

      // Execute the query with parameters
      results = (await connection).execute(query, {
        p_org_code: "50",
        p_fm_dt: fromDate,
        p_to_dt: toDate,
        branchCode: branchCode,
      });

      const formattedData = (await results).rows?.map((row: any) => ({
        org_code: row[0],
        branchCode: row[1],
        mc_code: row[2],
        class: row[3],
        paidAmount: row[4],
        retentionAmount: row[5],
        treatyAmount: row[6],
        comesaAmount: row[7],
        facAmount: row[8],
        xolAmount: row[9],
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
  async getARreceiptsListing(req: Request, res: Response) {
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
       SELECT hd_org_code,
         hd_index,
         hd_os_code,
         UPPER (pkg_sa.org_structure_name (hd_org_code, hd_os_code))
             branch,
         hd_no,
         TO_CHAR (hd_gl_date, 'dd/mm/rrrr')
             gl_date,
         hd_receipt_mgl_code,
         pkg_cust.get_mgl_bank_code (hd_org_code, hd_receipt_mgl_code)
             hd_bank_code,
         ln_mgl_code
             ln_mgl_code,
         ln_aent_code,
         ln_ent_code,
         hd_cur_code,
         hd_cur_code,
         hd_paying_for,
         NVL (hd_fc_amount, 0)
             reciept_amt,
         DECODE (hd_mode, 'CHEQUE', hd_mode || ' NO ' || hd_chq_no, hd_mode)
             hd_mode,
         hd_chq_no,
         hd_chq_bank,
         UPPER (
             NVL (pkg_system_admin.get_ent_catg_name (ln_aent_code), 'none'))
             category,
         UPPER (hd_remitter_from)
             paid_by,
         ln_ent_code
             on_account_code,
         UPPER (pkg_system_admin.get_entity_name (ln_aent_code, ln_ent_code))
             on_account_name,
         hd_cust_doc_ref_type
             receipt_type,
         do_doc_no
             doc_no,
         UPPER (NVL (ln_narration, hd_narration))
             hd_narration,
         UPPER (pkg_system_admin.get_user_name (ar_receipts_header.created_by))
             cashier,
         hd_status
    FROM ar_receipts_header,
         ar_receipt_lines,
         (  SELECT DISTINCT
                   do_org_code,
                   do_hd_no,
                   LISTAGG (do_doc_no, CHR (10) ON OVERFLOW TRUNCATE)
                       WITHIN GROUP (ORDER BY do_doc_no)    do_doc_no
              FROM (SELECT do_org_code, do_hd_no, do_doc_no FROM ar_receipt_docs)
          GROUP BY do_org_code, do_hd_no)
   WHERE     hd_org_code = ln_org_code(+)
         AND hd_no = ln_hd_no(+)
         AND hd_org_code = do_org_code(+)
         AND hd_no = do_hd_no(+)
         AND hd_complete = 'Y'
         AND hd_status NOT IN ('Cancelled')
         AND hd_posted = 'Y'
         AND hd_status IN ('Completed', 'Cancelled')
         AND hd_os_code = NVL ( :branchCode, hd_os_code)
         AND hd_no IN (SELECT DISTINCT trn_doc_no
                         FROM gl_transactions
                        WHERE trn_doc_type IN ('AR-RECEIPT'))
         AND hd_org_code = :p_org_code
         AND NVL (hd_banked, 'N') = NVL ( :p_banked, NVL (hd_banked, 'N'))
         AND TRUNC (hd_gl_date) BETWEEN ( :p_fm_dt) AND ( :p_to_dt)
ORDER BY hd_gl_date
      `;

      // Execute the query with parameters
      results = (await connection).execute(query, {
        p_banked: "N",
        p_org_code: "50",
        p_fm_dt: fromDate,
        p_to_dt: toDate,
        branchCode: branchCode,
      });

      const formattedData = (await results).rows?.map((row: any) => ({
        org_code: row[2],
        currencyCode: row[11],
        receiptAmount: row[14],
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
