import oracledb from "oracledb";

// Create and export the connection pool
const pool = oracledb.createPool({
  user: "icon",
  password: "b1ma",
  connectString: "localhost:1521/db19c",
  poolMin: 5,
  poolMax: 100,
  poolIncrement: 5,
});

export default pool;
