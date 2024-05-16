import oracledb from "oracledb";

// Create and export the connection pool
export const pool = oracledb.createPool({
  user: "ICON",
  password: "IC0N",
  connectionString: "192.168.1.112:1521/BIMA19C",
  poolMin: 5,
  poolMax: 100,
  poolIncrement: 5,
});

export default pool;
