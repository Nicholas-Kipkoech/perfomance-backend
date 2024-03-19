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
       SELECT 
    SUM(PR_LC_PREM + PR_LC_EARTQUAKE + PR_LC_POLITICAL) AS direct_premium,
    PR_OS_CODE,
    PR_INT_AENT_CODE 
FROM 
    uw_premium_register 
WHERE 
    EXTRACT(YEAR FROM pr_gl_date) = NVL(:year, EXTRACT(YEAR FROM pr_gl_date))
    AND pr_os_code = NVL(:branchCode, pr_os_code)
GROUP BY 
    PR_OS_CODE,
    PR_INT_AENT_CODE

      `;

      // Execute the query with parameters
      results = (await connection).execute(query, {
        year: year,
        branchCode: branchCode,
      });

      const formattedData = (await results).rows?.map((row: any) => ({
        totalPremium: row[0],
        branchCode: row[1],
        intermediaryCode: row[2],
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
