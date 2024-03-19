import { Request, Response } from "express";
import pool from "../database/database";

class PerformanceController {
  constructor() {}

  async getUnderwritingData(req: Request, res: Response) {
    let connection;
    let result;
    try {
      const { year, branchCode } = req.query;
      connection = (await pool).getConnection();
      console.log("connected to database");
      const results = (await connection).execute(
        `select sum(PR_LC_PREM+PR_LC_EARTQUAKE+PR_LC_POLITICAL) direct_premium ,
        PR_OS_CODE, PR_INT_AENT_CODE from uw_premium_register 
         where EXTRACT(YEAR FROM pr_gl_date) = ${year} and pr_os_code=${branchCode}
        GROUP BY 
    PR_OS_CODE,
    PR_INT_AENT_CODE 
`
      );
      result = (await results).rows;
      console.log(result);
      const formattedData = result?.map((row: any) => {
        return {
          totalPremium: row[0],
          branchCode: row[1],
          intermediaryCode: row[2],
          premiumDate: row[4],
        };
      });

      return res.status(200).json({ result: formattedData });
    } catch (error) {
      console.error();
    } finally {
      try {
        if (connection) {
          (await connection).close();
          console.info("connection closed success");
        }
      } catch (error) {
        console.error(error);
      }
    }
  }
}

const perfomanceController = new PerformanceController();

export default perfomanceController;
