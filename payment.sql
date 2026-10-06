\set invoice_id 5 * random(1,5400)
\set cashier_id random(29,30)
BEGIN;
SELECT id FROM school_demo.invoices WHERE id=:invoice_id FOR UPDATE;
INSERT INTO school_demo.payments(invoice_id,cashier_id,amount,paid_at,idempotency_key)
VALUES(:invoice_id,:cashier_id,0.01,clock_timestamp(),'loadtest-'||gen_random_uuid());
COMMIT;
