-- Create orders table (starts empty)
CREATE TABLE IF NOT EXISTS orders (
  order_id     SERIAL PRIMARY KEY,
  customer_id  INT NOT NULL,
  amount       NUMERIC(10,2) NOT NULL,
  status       VARCHAR(20) NOT NULL DEFAULT 'PENDING',
  created_at   TIMESTAMP NOT NULL DEFAULT NOW()
);
