import { Request, Response } from "express";
import pool from "../database/database";

class PerformanceController {
  constructor() {}

  private async fetchDataService(query: string) {
    let connection;
    try {
      connection = (await pool).getConnection();
      console.log("connected to database");

      const results = (await connection).execute(query);
      console.log((await results).rows);
      return (await results).rows;
    } catch (error) {
      console.error(error);
    } finally {
      if (connection) {
        try {
          (await connection).close();
          console.info("connection closed success");
        } catch (error) {
          console.error(error);
        }
      }
    }
  }

  async getPremiums(req: Request, res: Response) {
    try {
      const data = await this.fetchDataService(
        "select * from uw_premium_register"
      );
      return res.status(200).json({ data });
    } catch (error) {
      console.error();
    }
  }
}

const perfomanceController = new PerformanceController();

export default perfomanceController;
