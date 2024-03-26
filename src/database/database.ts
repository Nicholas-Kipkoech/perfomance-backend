import oracledb from "oracledb";

// Create and export the connection pool
export const pool = oracledb.createPool({
  user: "icon",
  password: "b1ma",
  connectString: "localhost:1521/db19c",
  // user: "icon",
  // password: "IC0N",
  // connectionString: "192.168.1.112:1521/bima19c",
  poolMin: 5,
  poolMax: 100,
  poolIncrement: 5,
});

export default pool;
