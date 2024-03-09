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
