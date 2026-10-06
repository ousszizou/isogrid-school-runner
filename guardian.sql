\set guardian_id random(1,1500)
BEGIN;
SELECT s.id,s.full_name,c.name AS class_name
FROM school_demo.guardian_students gs
JOIN school_demo.students s ON s.id=gs.student_id
JOIN school_demo.enrollments e ON e.student_id=s.id AND e.academic_year=2025
JOIN school_demo.classes c ON c.id=e.class_id
WHERE gs.guardian_id=:guardian_id;
SELECT i.id,i.amount_due,coalesce(p.paid,0) AS paid,i.amount_due-coalesce(p.paid,0) AS outstanding
FROM school_demo.guardian_students gs
JOIN school_demo.enrollments e ON e.student_id=gs.student_id AND e.academic_year=2025
JOIN school_demo.invoices i ON i.enrollment_id=e.id
LEFT JOIN LATERAL (SELECT sum(amount) AS paid FROM school_demo.payments WHERE invoice_id=i.id) p ON true
WHERE gs.guardian_id=:guardian_id ORDER BY i.due_date;
SELECT a.attended_on,a.status
FROM school_demo.guardian_students gs
JOIN school_demo.enrollments e ON e.student_id=gs.student_id AND e.academic_year=2025
JOIN school_demo.attendance a ON a.enrollment_id=e.id
WHERE gs.guardian_id=:guardian_id AND a.attended_on>=date '2026-04-01'
ORDER BY a.attended_on DESC LIMIT 60;
COMMIT;
