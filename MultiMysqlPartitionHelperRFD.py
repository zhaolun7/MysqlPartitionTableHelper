#-*- coding: utf-8 -*-
# author:zhaolun
# old script help managing partitioned table by reading `table.conf`, it should be run with crond.
# for better managing, save `table.conf` in mysql 

WLCM_INFO = "welcome to\n __  __                 _ ____            _   _ _   _             _   _      _\n|  \\/  |_   _ ___  __ _| |  _ \\ __ _ _ __| |_(_) |_(_) ___  _ __ | | | | ___| |_ __   ___ _ __\n| |\\/| | | | / __|/ _` | | |_) / _` | '__| __| | __| |/ _ \\| '_ \\| |_| |/ _ \\ | '_ \\ / _ \\ '__|\n| |  | | |_| \\__ \\ (_| | |  __/ (_| | |  | |_| | |_| | (_) | | | |  _  |  __/ | |_) |  __/ |\n|_|  |_|\\__, |___/\\__, |_|_|   \\__,_|_|   \\__|_|\\__|_|\\___/|_| |_|_| |_|\\___|_| .__/ \\___|_|\n        |___/        |_|                                                      |_|            v0.1"

import os,sys,commands,re,time,threading,logging
from datetime import datetime, timedelta
from optparse import OptionParser
SERVER_CONNENT = "mysql -h%(HOSTIP)s -P%(PORT)s -u%(USER)s -p%(PASSWORD)s -D%(DATABASE)s --default-character-set=utf8 -N -e '%(SQL)s' "
SQL_TRUNCATE   = "alter table %s truncate partition %s"
SQL_DROP       = "alter table %s drop partition %s"
SQL_ADD        = "alter table %s add partition (%s)"
#list_param = ["HOSTIP","PORT","USER","PASSWORD","DATABASE","TABLES","KEEP_DAY","TYPE","ACTION"]

THREAD_LOCK = threading.RLock()
#DEBUG_MODE = False
def getCurTime():
    return datetime.now().strftime('[%Y-%m-%d %H:%M:%S]')

def excuteSql(sectionMap,excute_sql):
    sectionMap = sectionMap.copy()
    sectionMap["SQL"] = excute_sql
    command_execute = SERVER_CONNENT % sectionMap
    
    status,output = commands.getstatusoutput(command_execute)
    #if status != 0:
    #    print '[ERROR]',output
    return status,output.replace('Warning: Using a password on the command line interface can be insecure.','')
def getAllPartitions(map_param, logger):

    status, output = excuteSql(map_param,"show create table " + map_param['TABLE'])
    if status != 0:
        logger.error("get partiton faild: msg=> %s",output)
        return None
    
    m = re.compile(r'PARTITION p_(\d+) VALUES LESS THAN \(\d+\)')
    partitions = []
    for line in output.split(r'\n'):
        r = m.search(line)
        if r is not None:
            partitions.append(r.group(1))
    return partitions


def checkAndGetSectionMap(option,logger):
    GET_SECTION_CONN = "mysql -h%s -P%s -u%s -p%s -D%s --default-character-set=utf8 -N -s -e '%s'" % (
        option.host,option.port,option.username,option.password,option.database,
        "select db_host,db_port,db_username,db_password,db_name,tb_name,tb_type,keep_day from t_partition_manage_tb tb join t_partition_manage_db db on tb.db_id = db.db_id")
    #print GET_SECTION_CONN
    #list_maps = 
    status,output = commands.getstatusoutput(GET_SECTION_CONN)
    if status != 0:
        return None
    lines = output.replace('Warning: Using a password on the command line interface can be insecure.','').split('\n')
    list_setctions = []
    for line in lines:
        if line == "":
            continue
        arr = line.split('\t')
        if len(arr) != 8:
            logger.error("err line:%s",line)
        #list_param = ["HOSTIP","PORT","USER","PASSWORD","DATABASE","TABLES","KEEP_DAY","TYPE","ACTION"]
        st_map = {}
        st_map["HOSTIP"]   = arr[0]
        st_map["PORT"]     = arr[1]
        st_map["USER"]     = arr[2]
        st_map["PASSWORD"] = arr[3]
        st_map["DATABASE"] = arr[4]
        st_map["TABLE"]    = arr[5]
        st_map["TYPE"]     = arr[6]
        st_map["KEEP_DAY"] = int(arr[7])
        if int(arr[7]) > 0:
            st_map["ACTION"] = ['TRUNCATE','DROP','ADD']
        else:
            st_map["ACTION"] = ['ADD']
        list_setctions.append(st_map)
    return list_setctions


def managePartition(query_id, map_param, str_tasktime, action, logger):
    table_type = map_param.get("TYPE")
    tasktime,str_endtime,str_deltime = None,None,None
    with THREAD_LOCK:
        tasktime = datetime.strptime(str_tasktime, '%Y%m%d')
        str_endtime = (tasktime + timedelta(days = 1)).strftime('%Y%m%d')
        str_deltime = (tasktime - timedelta(days = map_param['KEEP_DAY'])).strftime('%Y%m%d')

    sql = None
    sql_print = None
    all_partitions = getAllPartitions(map_param,logger)
    if all_partitions is None:
        return None,None
    if table_type == 'DAY':
        if action == 'ADD':
            if str_tasktime not in all_partitions:
                sql = SQL_ADD % (map_param.get('TABLE'), "PARTITION p_%s VALUES LESS THAN (%s)" % (str_tasktime, str_endtime))
            else:
                logger.info("[SKIP %s](%s) skip add table %s partition %s, already have.", action, query_id, map_param['TABLE'],str_tasktime)
                return None,None
        elif action == 'TRUNCATE':
            if str_deltime in all_partitions:
                sql = SQL_TRUNCATE % (map_param.get('TABLE'), 'p_' + str_deltime)
            else:
                logger.info('[SKIP %s](%s) skip truncate table %s partition %s not exits.', action, query_id, map_param['TABLE'], str_deltime)
                return None,None
        elif action == 'DROP':
            if str_deltime in all_partitions:
                sql = SQL_DROP % (map_param.get('TABLE'), 'p_' + str_deltime)
            else:
                logger.info('[SKIP %s](%s) skip drop table %s partition %s not exits.', action, query_id, map_param['TABLE'], str_deltime)
                return None,None
    elif table_type == 'HOUR':
        if action == 'ADD':
            if str_tasktime+'00' not in all_partitions:
                sql = SQL_ADD % (map_param.get('TABLE'), ', '.join([ '\n'*(1 if x%3==0 else 0)+ ' '*(10 if x%3==0 else 0) + "PARTITION p_%s VALUES LESS THAN (%s)" \
                    % (str_tasktime + str(x).zfill(2), str_tasktime + str(x+1).zfill(2)) for x in range(0,23) ]) \
                    + (", PARTITION p_%s23 VALUES LESS THAN (%s00)" % (str_tasktime,str_endtime)))
                sql_print = SQL_ADD % (map_param.get('TABLE'), 'PARTITION p_%s[00~23] VALUES LESS THAN (%s[01~23]|%s00)' % (str_tasktime,str_tasktime,str_endtime))
            else:
                logger.info('[SKIP %s](%s) skip add table %s partition %s[00~23], already have.', action, query_id, map_param['TABLE'], str_tasktime)
                return None,None
        elif action == 'TRUNCATE':
            if str_deltime+'00' in all_partitions:
                sql = SQL_TRUNCATE % (map_param.get('TABLE'), ','.join([ '\n'*(1 if x%6==0 else 0)+ ' '*(10 if x%6==0 else 1) + 'p_' + str_deltime + str(x).zfill(2) for x in range(0,24)]))
                sql_print = SQL_TRUNCATE % (map_param.get('TABLE'), str_deltime+'[00~23]')
            else:
                logger.info('[SKIP %s](%s) skip truncate table %s partition %s[00~23] not exits.', action, query_id, map_param['TABLE'],str_deltime)
                return None,None
        elif action == 'DROP':
            if str_deltime+'00' in all_partitions:
                sql = SQL_DROP % (map_param.get('TABLE'), ','.join([ '\n'*(1 if x%6==0 else 0)+ ' '*(10 if x%6==0 else 1) + 'p_' + str_deltime + str(x).zfill(2) for x in range(0,24)]))
                sql_print = SQL_DROP % (map_param.get('TABLE'), str_deltime+'[00~23]')
            else:
                logger.info('[SKIP %s](%s) skip drop table %s partition %s[00~23] not exits.', action, query_id, map_param['TABLE'],str_deltime)
                return None,None
    return sql,sql_print


class SingleTableActionThread (threading.Thread):
    def __init__(self, query_id, map_param, taskTime, logger):
        threading.Thread.__init__(self)
        self.query_id = query_id
        self.map_param = map_param
        self.taskTime = taskTime
        self.logger = logger
    def run(self):
        list_actions = self.map_param.pop("ACTION")
        for action in list_actions:
            sql,sql_print = managePartition(self.query_id,self.map_param,self.taskTime,action,self.logger)
            self.sql = sql
            query_id,logger = self.query_id,self.logger
            if sql is not None and sql != '':
                if sql_print != None:
                    logger.info('[EXEC %s](%s) %s', action, query_id, sql_print)
                else:
                    logger.info('[EXEC %s](%s) %s', action, query_id, sql)
                starttime = time.time()
                retcode,msg = excuteSql(self.map_param,sql)
                endtime = time.time()
                usetime = endtime - starttime
                if retcode == 0:
                    logger.info('[SUCC %s](%s) [RETCODE:%d, msg=> %s][usedTime:%ss]', action, query_id, retcode, msg,usetime)
                else:
                    logger.error('[FAIL %s](%s) [RETCODE:%d, msg=> %s][usedTime:%ss],sql is: %s', action, query_id, retcode, msg,usetime, self.sql)
        

def main(argv=None):
    logging.basicConfig(level=logging.INFO, 
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=os.path.join(os.path.split(os.path.realpath(__file__))[0],'crond.log'),
                        format='%(asctime)s [%(levelname)s] %(message)s')
    logger = logging.getLogger(__name__)
    taskTime = datetime.now().strftime('%Y%m%d') 
    logger.info('%s',WLCM_INFO)
    
    optParser = OptionParser()
    optParser.add_option('--host',action = 'store',type = "string" ,dest = 'host')
    optParser.add_option('-P','--port',action = 'store',type = "int" ,dest = 'port')
    optParser.add_option('-u','--username',action = 'store',type = "string" ,dest = 'username')
    optParser.add_option('-p','--password',action = 'store',type = "string" ,dest = 'password')
    optParser.add_option('-D','--database',action = 'store',type = "string" ,dest = 'database')
    optParser.add_option('--tasktime',action = 'store',type = "string" ,dest = 'tasktime')
    option,args = optParser.parse_args()
    #simple check start #
    if option.host is None or option.host == "":
        logger.info("invalid host:%s",option.host)
        return -1
    if option.port is None or option.port == "":
        logger.info("invalid port:%s",option.port)
        return -1
    if option.username is None or option.username == "":
        logger.info("invalid username:%s",option.username)
        return -1
    if option.password is None or option.password == "":
        logger.info("invalid password:%s",option.password)
        return -1
    if option.database is None or option.database == "":
        logger.info("invalid database:%s",option.database)
        return -1
    #simple check end  #
    if option.tasktime is not None and option.tasktime != "":
        taskTime = option.tasktime
        if len(option.tasktime) != 8:
            logger.error('taskTime error: %s',taskTime)
            return -1
    else:
        option.tasktime = taskTime
    logger.info('taskTime is %s',taskTime)
    
    list_maps =  checkAndGetSectionMap(option,logger)

    if list_maps is None:
        logger.error('Error when read configs')
        return -1
    tableCnt = 0
    for map_param in list_maps:
        t = SingleTableActionThread("T-%04d" % tableCnt, map_param, taskTime, logger)
        while threading.activeCount() > 10:
            time.sleep(0.1)
        t.start()
        tableCnt += 1
    time.sleep(2)
    while threading.activeCount() != 1:
        time.sleep(10)
        if threading.activeCount() != 1:
            list_t = threading.enumerate()
            for t in list_t:
                if t.getName() != 'MainThread':
                    if t.sql_print is not None:
                        logger.info('[WAIT SQL] wait query:%s', t.sql_print)
                    else:
                        logger.info('[WAIT SQL] wait query:%s', t.sql)
    logger.info('script exit.')

if __name__ == "__main__":
    sys.exit(main(sys.argv))