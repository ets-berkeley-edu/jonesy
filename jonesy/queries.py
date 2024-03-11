advisor_notes_access = """
    SELECT
        A.USER_ID,
        A.CS_ID,
        A.PERMISSION_LIST
    FROM SYSADM.BOA_ADV_NOTES_ACCESS_VW A"""


instructor_advisor_relationships = """
    SELECT DISTINCT
        I.ADVISOR_ID,
        I.CAMPUS_ID,
        I.INSTRUCTOR_ADISOR_NUMBER AS INSTRUCTOR_ADVISOR_NBR,
        I.ADVISOR_TYPE,
        I.ADVISOR_TYPE_DESCR,
        I.INSTRUCTOR_TYPE,
        I.INSTRUCTOR_TYPE_DESCR,
        I.ACADEMIC_PROGRAM,
        I.ACADEMIC_PROGRAM_DESCR,
        I.ACADEMIC_PLAN,
        I.ACADEMIC_PLAN_DESCR,
        I.ACADEMIC_SUB_PLAN,
        I.ACADEMIC_SUB_PLAN_DESCR
    FROM SYSADM.BOA_INSTRUCTOR_ADVISOR_VW I
    WHERE I.INSTITUTION = 'UCB01'
        AND I.ACADEMIC_CAREER = 'UGRD'
        AND I.EFFECTIVE_STATUS = 'A'
        AND I.EFFECTIVE_DATE = (
            SELECT MAX(I1.EFFECTIVE_DATE)
            FROM SYSADM.BOA_INSTRUCTOR_ADVISOR_VW I1
            WHERE I1.ADVISOR_ID = I.ADVISOR_ID
            AND I1.INSTRUCTOR_ADISOR_NUMBER = I.INSTRUCTOR_ADISOR_NUMBER
        )"""


# Late withdrawals are only indicated in primary section enrollments, and do not change
# any values in secondary section enrollment rows. The CASE clause implements a
# conditional join for secondary sections.
omit_drops_and_withdrawals = """
    enroll.STDNT_ENRL_STATUS_CODE != 'D' AND
    CASE enroll.GRADING_BASIS_CODE
    WHEN 'NON' THEN (
        SELECT MIN(prim_enr.GRADE_MARK)
        FROM SISEDO.CLASSSECTIONALLV01_MVW sec
        LEFT JOIN SISEDO.ETS_ENROLLMENTV01_VW prim_enr
            ON prim_enr.CLASS_SECTION_ID = sec."primaryAssociatedSectionId"
            AND prim_enr.TERM_ID = enroll.TERM_ID
            AND prim_enr.STUDENT_ID = enroll.STUDENT_ID
            AND prim_enr.STDNT_ENRL_STATUS_CODE != 'D'
         WHERE sec."id" = enroll.CLASS_SECTION_ID
            AND sec."term-id" = enroll.TERM_ID
            AND prim_enr.STUDENT_ID IS NOT NULL
    )
    ELSE enroll.GRADE_MARK END != 'W'"""


# See http://www.oracle.com/technetwork/issue-archive/2006/06-sep/o56asktom-086197.html for explanation of
# query batching with ROWNUM.
def get_batch_basic_attributes(batch_number, batch_size):
    mininum_row_exclusive = (batch_number * batch_size)
    maximum_row_inclusive = mininum_row_exclusive + batch_size
    return f"""
        SELECT ldap_uid, sid, first_name, last_name, email_address, affiliations, person_type, alternateid
            FROM (SELECT /*+ FIRST_ROWS(n) */ attributes.*, ROWNUM rnum
                FROM (SELECT
                    pi.ldap_uid, pi.student_id AS sid, TRIM(pi.first_name) AS first_name, TRIM(pi.last_name) as last_name,
                    pi.email_address, pi.affiliations, pi.person_type, pi.alternateid
                    FROM SISEDO.CALCENTRAL_PERSON_INFO_VW pi
                    WHERE person_type != 'Z'
                    ORDER BY pi.ldap_uid
                ) attributes
            WHERE ROWNUM <= {maximum_row_inclusive})
        WHERE rnum > {mininum_row_exclusive}"""


def get_recent_enrollment_updates(term_id, recency_cutoff):
    return f"""
        SELECT DISTINCT
            enroll.CLASS_SECTION_ID as section_id,
            enroll.TERM_ID as term_id,
            enroll.CAMPUS_UID AS ldap_uid,
            enroll.STUDENT_ID AS sis_id,
            enroll.STDNT_ENRL_STATUS_CODE AS enroll_status,
            enroll.COURSE_CAREER AS course_career,
            enroll.LAST_UPDATED as last_updated
        FROM SISEDO.ETS_ENROLLMENTV01_VW enroll
        WHERE enroll.TERM_ID = {term_id}
        AND {omit_drops_and_withdrawals}
        AND enroll.last_updated >= to_timestamp('{recency_cutoff.strftime('%Y-%m-%d %H:%M:%S')}', 'yyyy-mm-dd hh24:mi:ss')
        ORDER BY enroll.TERM_ID,
            -- In case the number of results exceeds our processing cutoff, set priority within terms by the academic
            -- career type for the course.
            CASE
                WHEN enroll.course_career = 'UGRD' THEN 1
                WHEN enroll.course_career = 'GRAD' THEN 2
                WHEN enroll.course_career = 'LAW' THEN 3
                WHEN enroll.course_career = 'UCBX' THEN 4
                ELSE 5
            END,
            enroll.CLASS_SECTION_ID, enroll.CAMPUS_UID, enroll.last_updated DESC"""


def get_recent_instructor_updates(term_id, recency_cutoff):
    return f"""
        SELECT DISTINCT
            up.instr_id AS sis_id,
            up.term_id,
            up.class_section_id AS section_id,
            up.crse_id AS course_id,
            instr."campus-uid" AS ldap_uid,
            instr."role-code" AS role_code,
            sec."primary",
            up.last_updated
            FROM SISEDO.CLASS_INSTR_UPDATESV00_VW up
            JOIN SISEDO.ASSIGNEDINSTRUCTORV00_VW instr ON (
                instr."cs-course-id" = up.crse_id AND
                instr."term-id" = up.term_id AND
                instr."session-id" = up.session_code AND
                instr."offeringNumber" = up.crse_offer_nbr AND
                instr."number" = up.class_section
            )
            JOIN SISEDO.CLASSSECTIONALLV01_MVW sec ON (
                sec."id" = up.class_section_id AND sec."term-id" = up.term_id
            )
            WHERE up.change_type IN ('C', 'U') AND up.term_id= {term_id} AND
            up.last_updated >= to_timestamp('{recency_cutoff.strftime('%Y-%m-%d %H:%M:%S')}', 'yyyy-mm-dd hh24:mi:ss')
            ORDER BY up.term_id, up.crse_id, up.class_section_id, instr."campus-uid", up.last_updated DESC"""


# Get the undergraduate term in progress, plus the next two. Ripley code on the other side of the pipeline will
# validate how many of these should in fact be considered 'current.'
current_terms = """
    SELECT * FROM (
        SELECT DISTINCT term_id FROM SISEDO.CLC_TERMV00_VW WHERE term_id >= (
            SELECT MAX(term_id) from SISEDO.CLC_TERMV00_VW where term_id < (
                SELECT MIN(term_id)
                FROM SISEDO.CLC_TERMV00_VW
                WHERE institution = 'UCB01' AND
                    acadcareer_code = 'UGRD' AND
                    term_type IS NOT NULL AND
                    term_begin_dt > CURRENT_DATE
            )
        ) ORDER BY term_id
    ) WHERE rownum <= 3"""
