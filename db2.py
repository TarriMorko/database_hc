
from collections import namedtuple
DailyDB2Data = namedtuple("DailyDB2Data", "current_date,\
    current_time,\
    TOTAL_ACT_TIME,\
    ACT_ABORTED_TOTAL,\
    ACT_COMPLETED_TOTAL,\
    ACT_REJECTED_TOTAL,\
    ROWS_RETURNED,\
    ROWS_UPDATED,\
    ROWS_DELETED,\
    ROWS_READ,\
    TOTAL_WAIT_TIME,\
    TOTAL_RQST_TIME,\
    POOL_DATA_P_READS,\
    POOL_INDEX_P_READS,\
    POOL_TEMP_DATA_P_READS,\
    POOL_TEMP_INDEX_P_READS,\
    POOL_TEMP_XDA_P_READS,\
    POOL_XDA_P_READS,\
    POOL_DATA_L_READS,\
    POOL_INDEX_L_READS,\
    POOL_TEMP_DATA_L_READS,\
    POOL_TEMP_INDEX_L_READS,\
    POOL_TEMP_XDA_L_READS,\
    POOL_XDA_L_READS,\
    ROWS_MODIFIED,\
    ROWS_RETURNED2,\
    ACT_ABORTED_TOTAL2,\
    ACT_COMPLETED_TOTAL2,\
    ACT_REJECTED_TOTAL2,\
    PKG_CACHE_INSERTS,\
    PKG_CACHE_LOOKUPS,\
    CAT_CACHE_INSERTS,\
    CAT_CACHE_LOOKUPS,\
    SORT_OVERFLOWS,\
    TOTAL_SORTS")

# DailyDB2Message = namedtuple("DailyDB2Message", "SQL_response_time,\
#     eff_read,\
#     wait_time,\
#     bp_hit_rate,\
#     avg_rr,\
#     pkg_hit_rate,\
#     sortoverflows_rate")

file="HealthCheck_CUB_ELOAN_inst411_11.txt"
with open(file, 'rt') as f:
    DailyDB2Data = [ x for x in f if len(x) > 1 ]
# a=aline[-1]

# 一筆資料會像這樣
# '11/16/2018 11:37:05 0 0 1 0 1 0 0 620 115 464 139 234 0 0 0 0 538 666 0 0 0 0 0 1 0 1 0 1 1 33 40 0 0\n'


def create_one_record(one_line_in_txt_file):
    # 讀進一筆原始資料，回傳一筆整理後的資料
    #
    line = one_line_in_txt_file.split()
    nl = []
    for nu, el in enumerate(line):
        if nu < 2:
            nl.append(el)
        else:
            nl.append(int(el))

    try:
        SQL_response_time=nl[2]/nl[3]+nl[4]+nl[5]
    except ZeroDivisionError:
        SQL_response_time=0

    try:
        eff_read=nl[6]/nl[7]+nl[8]+nl[9]
    except ZeroDivisionError:
        eff_read=0

    try:
        wait_time=nl[10]/nl[11]
    except ZeroDivisionError:
        wait_time=0

    try:
        bp_hit_rate= 1 - (nl[12] + nl[13] + nl[14] + nl[15] + nl[16] + nl[17])/(nl[18] + nl[19] + nl[20] + nl[21] + nl[22] + nl[23])
    except ZeroDivisionError:
        bp_hit_rate=0

    try:
        avg_rr=(nl[24] + nl[25]) / (nl[26] + nl[27] + nl[28])
    except ZeroDivisionError:
        avg_rr=0

    try:
        pkg_hit_rate= nl[29] / nl[30]
    except ZeroDivisionError:
        pkg_hit_rate=0

    try:
        catalog_hit_rate= nl[31] / nl[32]
    except ZeroDivisionError:
        catalog_hit_rate=0

    try:
        sortoverflows_rate= nl[33] / nl[34]
    except ZeroDivisionError:
        sortoverflows_rate=0
   

    return (nl[0], nl[1], SQL_response_time, eff_read, wait_time, bp_hit_rate, avg_rr, pkg_hit_rate, catalog_hit_rate, sortoverflows_rate)
    # line[0]  # 我這邊做好一筆之後，再 append 到 DailyDB2Message

    # line[0] current date,
    # line[1] current time,
    # line[2] TOTAL_ACT_TIME,
    # line[3] ACT_ABORTED_TOTAL,
    # line[4] ACT_COMPLETED_TOTAL,
    # line[5] ACT_REJECTED_TOTAL,
    # line[6] ROWS_RETURNED,
    # line[7] ROWS_UPDATED,
    # line[8] ROWS_DELETED,
    # line[9] ROWS_READ,
    # line[10] TOTAL_WAIT_TIME,
    # line[11] TOTAL_RQST_TIME,
    # line[12] POOL_DATA_P_READS,
    # line[13] POOL_INDEX_P_READS,
    # line[14] POOL_TEMP_DATA_P_READS,
    # line[15] POOL_TEMP_INDEX_P_READS,
    # line[16] POOL_TEMP_XDA_P_READS,
    # line[17] POOL_XDA_P_READS,
    # line[18] POOL_DATA_L_READS,
    # line[19] POOL_INDEX_L_READS,
    # line[20] POOL_TEMP_DATA_L_READS,
    # line[21] POOL_TEMP_INDEX_L_READS,
    # line[22] POOL_TEMP_XDA_L_READS,
    # line[23] POOL_XDA_L_READS,
    # line[24] ROWS_MODIFIED,
    # line[25] ROWS_RETURNED,
    # line[26] ACT_ABORTED_TOTAL,
    # line[27] ACT_COMPLETED_TOTAL,
    # line[28] ACT_REJECTED_TOTAL,
    # line[29] PKG_CACHE_INSERTS,
    # line[30] PKG_CACHE_LOOKUPS,
    # line[31] CAT_CACHE_INSERTS,
    # line[32] CAT_CACHE_LOOKUPS,
    # line[33] SORT_OVERFLOWS,
    # line[34] TOTAL_SORTS,


DailyDB2Message = []
for line in DailyDB2Data:
    DailyDB2Message.append(create_one_record(line))
