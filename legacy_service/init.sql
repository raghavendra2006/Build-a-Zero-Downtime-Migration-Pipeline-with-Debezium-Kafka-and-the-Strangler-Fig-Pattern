-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
  order_id     SERIAL PRIMARY KEY,
  customer_id  INT NOT NULL,
  amount       NUMERIC(10,2) NOT NULL,
  status       VARCHAR(20) NOT NULL DEFAULT 'PENDING',
  created_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Seed 5000 historical order records
INSERT INTO orders (customer_id, amount, status, created_at)
SELECT
  (random() * 999 + 1)::INT AS customer_id,
  round((random() * 500 + 5)::NUMERIC, 2) AS amount,
  CASE (random() * 4)::INT
    WHEN 0 THEN 'PENDING'
    WHEN 1 THEN 'CONFIRMED'
    WHEN 2 THEN 'SHIPPED'
    WHEN 3 THEN 'DELIVERED'
    ELSE 'CANCELLED'
  END AS status,
  NOW() - (random() * INTERVAL '90 days') AS created_at
FROM generate_series(1, 5000);
