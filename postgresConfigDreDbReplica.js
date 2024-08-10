const { readFileSync } = require('fs');
module.exports = {
  max: 20,
  application_name: process.env.MY_NAME_IS,
  host: process.env.REPLICA_PG_HOST,
  user: process.env.PG_USER_DRE,
  password: process.env.PG_USER_DRE_PASSWORD,
  database: process.env.PG_DATABASE.toLowerCase(),
  idle_in_transaction_session_timeout: 300000,
  port: process.env.PG_PORT,
  ...(process.env.PG_SSL === 'true'
    ? {
        ssl: {c
          rejectUnauthorized: false,
          ca: readFileSync('certs/replica/ca.pem').toString(),
          key: readFileSync('certs/replica/dre/key.pem').toString(),
          cert: readFileSync('certs/replica/dre/cert.pem').toString()
        }
      }
    : '')
};
