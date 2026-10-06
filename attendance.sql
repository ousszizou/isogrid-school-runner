\set class_id random(1,40)
BEGIN;
INSERT INTO school_demo.attendance(enrollment_id,attended_on,status,recorded_by)
SELECT e.id,date '2026-10-06','present',c.homeroom_staff_id
FROM school_demo.enrollments e JOIN school_demo.classes c ON c.id=e.class_id
WHERE e.academic_year=2025 AND e.class_id=:class_id ORDER BY e.id
ON CONFLICT(enrollment_id,attended_on) DO UPDATE
SET status=excluded.status,recorded_by=excluded.recorded_by;
COMMIT;
