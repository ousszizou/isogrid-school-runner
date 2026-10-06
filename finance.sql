BEGIN;
WITH paid AS (
 SELECT p.invoice_id,sum(p.amount) AS amount
 FROM school_demo.payments p
 JOIN school_demo.invoices i ON i.id=p.invoice_id
 JOIN school_demo.enrollments e ON e.id=i.enrollment_id
 WHERE e.academic_year=2025 GROUP BY p.invoice_id
)
SELECT c.id,c.name,count(*) AS invoices,sum(i.amount_due) AS due,
sum(coalesce(p.amount,0)) AS paid,sum(i.amount_due-coalesce(p.amount,0)) AS outstanding
FROM school_demo.enrollments e JOIN school_demo.classes c ON c.id=e.class_id
JOIN school_demo.invoices i ON i.enrollment_id=e.id
LEFT JOIN paid p ON p.invoice_id=i.id
WHERE e.academic_year=2025 GROUP BY c.id,c.name ORDER BY c.id;
COMMIT;
